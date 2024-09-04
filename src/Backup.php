<?php

declare(strict_types=1);

namespace Keboola\ProjectBackup;

use Exception;
use GuzzleHttp\Client as GuzzleClient;
use Keboola\ProjectBackup\Exception\SkipTableException;
use Keboola\ProjectBackup\FileClient\AbsFileClient;
use Keboola\ProjectBackup\FileClient\IFileClient;
use Keboola\ProjectBackup\FileClient\S3FileClient;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\DevBranchesMetadata;
use Keboola\StorageApi\HandlerStack;
use Keboola\StorageApi\Options\Components\ListConfigurationMetadataOptions;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use stdClass;

abstract class Backup
{
    private const CONFIGURATION_PAGING_LIMIT = 2;

    protected LoggerInterface $logger;

    protected Client $sapiClient;

    protected BranchAwareClient $branchAwareClient;

    public function __construct(Client $sapiClient, ?LoggerInterface $logger)
    {
        $this->sapiClient = $sapiClient;
        $this->logger = $logger ?: new NullLogger();

        $devBranches = new DevBranches($this->sapiClient);
        $listBranches = $devBranches->listBranches();
        $defaultBranch = current(array_filter($listBranches, fn($v) => $v['isDefault'] === true));

        $this->branchAwareClient = new BranchAwareClient(
            $defaultBranch['id'],
            [
                'url' => $sapiClient->getApiUrl(),
                'token' => $sapiClient->getTokenString(),
            ],
        );
    }

    /**
     * @param string|resource $content
     */
    abstract protected function putToStorage(string $name, $content): void;

    public function backupProjectMetadata(): void
    {
        $devBranchMetadata = new DevBranchesMetadata($this->branchAwareClient);

        $this->putToStorage(
            'defaultBranchMetadata.json',
            (string) json_encode($devBranchMetadata->listBranchMetadata()),
        );
    }

    public function backupTable(string $tableId): void
    {
        try {
            $fileInfo = $this->getTableFileInfo($tableId);
        } catch (SkipTableException $e) {
            return;
        }

        $fileClient = $this->getFileClient($fileInfo);
        if ($fileInfo['isSliced'] === true) {
            // Download manifest with all sliced files
            $client = new GuzzleClient([
                'handler' => HandlerStack::create([
                    'backoffMaxTries' => 10,
                ]),
            ]);
            /** @var array $manifest */
            $manifest = json_decode($client->get($fileInfo['url'])->getBody()->getContents(), true);

            /**
             * @var int $i
             * @var array $part
             */
            foreach ($manifest['entries'] as $i => $part) {
                $this->putToStorage(
                    sprintf(
                        '%s.part_%d.csv.gz',
                        str_replace('.', '/', $tableId),
                        $i,
                    ),
                    $fileClient->getFileContent($part),
                );
            }
        } else {
            $this->putToStorage(
                str_replace('.', '/', $tableId) . '.csv.gz',
                $fileClient->getFileContent(),
            );
        }
    }

    protected function getTableFileInfo(string $tableId): array
    {
        $table = $this->sapiClient->getTable($tableId);

        if ($table['bucket']['stage'] === 'sys') {
            $this->logger->warning(sprintf('Skipping table %s (sys bucket)', $table['id']));
            throw new SkipTableException();
        }

        if ($table['bucket']['hasExternalSchema']) {
            $this->logger->warning(sprintf('Skipping table %s (external schema)', $table['id']));
            throw new SkipTableException();
        }

        if (!empty($table['bucket']['sourceBucket'])) {
            $this->logger->warning(sprintf('Skipping table %s (Data Catalog)', $table['id']));
            throw new SkipTableException();
        }

        if ($table['isAlias']) {
            $this->logger->warning(sprintf('Skipping table %s (alias)', $table['id']));
            throw new SkipTableException();
        }

        $this->logger->info(sprintf('Exporting table %s', $tableId));

        $fileId = $this->sapiClient->exportTableAsync($tableId, [
            'gzip' => true,
        ]);

        return (array) $this->sapiClient->getFile(
            $fileId['file']['id'],
            (new GetFileOptions())->setFederationToken(true),
        );
    }

    public function backupTablesMetadata(): void
    {
        $this->logger->info('Exporting buckets');

        $buckets = $this->sapiClient->listBuckets(['include' => 'metadata']);
        $buckets = array_filter($buckets, fn($bucket) => empty($bucket['sourceBucket']));
        $buckets = array_filter($buckets, fn($bucket) => $bucket['hasExternalSchema'] === false);

        $this->putToStorage(
            'buckets.json',
            (string) json_encode($buckets),
        );

        $this->logger->info('Exporting tables');
        $tables = $this->sapiClient->listTables(null, [
            'include' => 'columns,buckets,metadata,columnMetadata',
        ]);
        $tables = array_filter($tables, fn($table) => empty($table['bucket']['sourceBucket']));

        $this->putToStorage('tables.json', (string) json_encode($tables));
    }

    public function backupConfigs(bool $includeVersions = true): void
    {
        $this->logger->info('Exporting configurations');

        $tmp = new Temp();

        $configurationsFile = $tmp->createFile('configurations.json');
        $versionsFile = $tmp->createFile('versions.json');

        // use raw api call to prevent parsing json - preserve empty JSON objects
        $this->sapiClient->apiGet('components?include=configuration', $configurationsFile->getPathname());
        $handle = fopen((string) $configurationsFile, 'r');
        if ($handle) {
            $this->putToStorage('configurations.json', (string) stream_get_contents($handle));
            fclose($handle);
        } else {
            throw new Exception(sprintf('Cannot open file %s', (string) $configurationsFile));
        }

        $url = 'components';
        $url .= '?include=configuration,rows,state';
        $this->sapiClient->apiGet($url, $configurationsFile->getPathname());
        /** @var array $configurations */
        $configurations = json_decode((string) file_get_contents($configurationsFile->getPathname()));

        $limit = self::CONFIGURATION_PAGING_LIMIT;

        /** @var stdClass $component */
        foreach ($configurations as $component) {
            $this->logger->info(sprintf('Exporting %s configurations', $component->id));

            foreach ($component->configurations as $configuration) {
                if ($includeVersions) {
                    $offset = 0;
                    $versions = [];
                    do {
                        $url = "components/{$component->id}/configs/{$configuration->id}/versions";
                        $url .= '?include=name,description,configuration,state';
                        $url .= "&limit={$limit}&offset={$offset}";
                        $this->sapiClient->apiGet($url, $versionsFile->getPathname());
                        /** @var array $versionsTmp */
                        $versionsTmp = json_decode((string) file_get_contents($versionsFile->getPathname()));

                        $versions = array_merge($versions, $versionsTmp);
                        $offset = $offset + $limit;
                    } while (count($versionsTmp) > 0);
                    $configuration->_versions = $versions;
                }
                if ($includeVersions) {
                    foreach ($configuration->rows as &$row) {
                        $offset = 0;
                        $versions = [];
                        do {
                            $url = "components/{$component->id}";
                            $url .= "/configs/{$configuration->id}";
                            $url .= "/rows/{$row->id}/versions";
                            $url .= '?include=configuration';
                            $url .= "&limit={$limit}&offset={$offset}";
                            $this->sapiClient->apiGet($url, $versionsFile->getPathname());
                            /** @var array $versionsTmp */
                            $versionsTmp = json_decode((string) file_get_contents($versionsFile->getPathname()));
                            $versions = array_merge($versions, $versionsTmp);
                            $offset = $offset + $limit;
                        } while (count($versionsTmp) > 0);
                        $row->_versions = $versions;
                    }
                }
                $this->putToStorage(
                    sprintf(
                        'configurations/%s/%s.json',
                        $component->id,
                        $configuration->id,
                    ),
                    (string) json_encode($configuration),
                );
                $metadata = new ListConfigurationMetadataOptions();
                $metadata->setComponentId($component->id);
                $metadata->setConfigurationId($configuration->id);

                $componentClass = new Components($this->branchAwareClient);

                $metadataData = $componentClass->listConfigurationMetadata($metadata);
                if (!empty($metadataData)) {
                    $this->putToStorage(
                        sprintf(
                            'configurations/%s/%s.json.metadata',
                            $component->id,
                            $configuration->id,
                        ),
                        (string) json_encode($metadataData),
                    );
                }
            }
        }
    }

    public function backupPermanentFiles(): void
    {
        $this->logger->info('Exporting permanent files');

        $files = $this->sapiClient->listFiles();
        $permanentFiles = [];
        foreach ($files as $file) {
            if (!is_null($file['maxAgeDays'])) {
                continue;
            }

            $this->putToStorage(
                sprintf('files/%s', $file['name']),
                (string) file_get_contents($file['url']),
            );
            $permanentFiles[] = [
                'name'=> $file['name'],
                'tags' => $file['tags'],
            ];
        }

        $this->putToStorage('permanentFiles.json', (string) json_encode($permanentFiles));
    }

    public function backupTriggers(): void
    {
        $this->logger->info('Exporting triggers');

        $triggers = $this->sapiClient->listTriggers();

        $this->putToStorage('triggers.json', (string) json_encode($triggers));
    }

    public function backupNotifications(): void
    {
        $this->logger->info('Exporting notifications');

        $notificationClient = new NotificationClient(
            $this->sapiClient->getServiceUrl('notification'),
            $this->sapiClient->getTokenString(),
            [
                'backoffMaxTries' => 3,
                'userAgent' => 'Keboola Project Backup',
            ],
        );
        $devBranches = new DevBranches($this->sapiClient);
        $listBranches = $devBranches->listBranches();
        $defaultBranch = current(array_filter($listBranches, fn($v) => $v['isDefault'] === true));

        $notifications = [];
        foreach ($notificationClient->listSubscriptions() as $subscription) {
            foreach ($subscription['filters'] as $item) {
                if ($item['field'] === 'branch.id' && $item['value'] !== strval($defaultBranch['id'])) {
                    continue 2;
                }
            }
            $notifications[] = $subscription;
        }

        $this->putToStorage('notifications.json', (string) json_encode($notifications));
    }

    protected function getFileClient(array $fileInfo): IFileClient
    {
        if (isset($fileInfo['credentials'])) {
            return new S3FileClient($fileInfo);
        } elseif (isset($fileInfo['absCredentials'])) {
            return new AbsFileClient($fileInfo);
        } else {
            throw new Exception('Unknown file storage client.');
        }
    }
}
