<?php

declare(strict_types=1);

namespace Keboola\ProjectBackup;

use Aws\S3\S3Client;
use Keboola\StorageApi\Client as StorageApi;
use Keboola\StorageApi\HandlerStack;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Symfony\Component\Filesystem\Filesystem;

class S3Backup
{
    private const CONFIGURATION_PAGING_LIMIT = 2;

    /**
     * @var StorageApi
     */
    private $sapiClient;

    /**
     * @var S3Client
     */
    private $s3Client;

    /**
     * @var LoggerInterface
     */
    private $logger;

    public function __construct(StorageApi $sapiClient, S3Client $s3Client, ?LoggerInterface $logger = null)
    {
        $this->sapiClient = $sapiClient;
        $this->s3Client = $s3Client;
        $this->logger = $logger?: new NullLogger();
    }

    private function trimTargetBasePath(?string $targetBasePath = null): string
    {
        if (empty($targetBasePath) || $targetBasePath === '/') {
            return '';
        } else {
            return trim($targetBasePath, '/') . '/';
        }
    }

    public function backupTablesMetadata(string $targetBucket, ?string $targetBasePath = null): void
    {
        $targetBasePath = $this->trimTargetBasePath($targetBasePath);
        $this->logger->info('Exporting buckets');

        $this->s3Client->putObject([
            'Bucket' => $targetBucket,
            'Key' => $targetBasePath . 'buckets.json',
            'Body' => json_encode($this->sapiClient->listBuckets(['include' => 'attributes,metadata'])),
        ]);

        $this->logger->info('Exporting tables');
        $tables = $this->sapiClient->listTables(null, [
            'include' => 'attributes,columns,buckets,metadata,columnMetadata',
        ]);

        $this->s3Client->putObject([
            'Bucket' => $targetBucket,
            'Key' => $targetBasePath . 'tables.json',
            'Body' => json_encode($tables),
        ]);
    }

    public function backupTable(string $tableId, string $targetBucket, ?string $targetBasePath = null): void
    {
        $table = $this->sapiClient->getTable($tableId);

        if ($table['bucket']['stage'] === 'sys') {
            $this->logger->warning(sprintf('Skipping table %s (sys bucket)', $table['id']));
            return;
        }

        if ($table['isAlias']) {
            $this->logger->warning(sprintf('Skipping table %s (alias)', $table['id']));
            return;
        }

        $targetBasePath = $this->trimTargetBasePath($targetBasePath);
        $this->logger->info(sprintf('Exporting table %s', $tableId));

        $tmp = new Temp();
        $tmp->initRunFolder();

        $fileId = $this->sapiClient->exportTableAsync($tableId, [
            'gzip' => true,
        ]);
        $fileInfo = $this->sapiClient->getFile($fileId['file']['id'], (new GetFileOptions())->setFederationToken(true));

        // Initialize S3Client with credentials from Storage API
        $s3Client = new S3Client([
            'version' => 'latest',
            'region' => $fileInfo['region'],
            'credentials' => [
                'key' => $fileInfo['credentials']['AccessKeyId'],
                'secret' => $fileInfo['credentials']['SecretAccessKey'],
                'token' => $fileInfo['credentials']['SessionToken'],
            ],
        ]);

        $fs = new Filesystem();
        if ($fileInfo['isSliced'] === true) {
            // Download manifest with all sliced files
            $client = new \GuzzleHttp\Client([
                'handler' => HandlerStack::create([
                    'backoffMaxTries' => 10,
                ]),
            ]);
            $manifest = json_decode($client->get($fileInfo['url'])->getBody()->getContents(), true);

            // Download all slices
            //@FIXME better temps
            $tmpFilePath = $tmp->getTmpFolder() . DIRECTORY_SEPARATOR . uniqid('sapi-export-');
            foreach ($manifest['entries'] as $i => $part) {
                $fileKey = substr($part['url'], strpos($part['url'], '/', 5) + 1);
                $filePath = $tmpFilePath . '_' . md5(str_replace('/', '_', $fileKey));
                $s3Client->getObject(array(
                    'Bucket' => $fileInfo['s3Path']['bucket'],
                    'Key' => $fileKey,
                    'SaveAs' => $filePath
                ));
                $fh = fopen($filePath, 'r');
                if ($fh) {
                    $this->s3Client->putObject([
                        'Bucket' => $targetBucket,
                        'Key' => $targetBasePath . str_replace('.', '/', $tableId) . '.part_' . $i . '.csv.gz',
                        'Body' => $fh,
                    ]);
                    fclose($fh);
                } else {
                    throw new \Exception(sprintf('Cannot open file %s', $filePath));
                }
                $fs->remove($filePath);
            }
        } else {
            $tmpFilePath = $tmp->getTmpFolder() . DIRECTORY_SEPARATOR . uniqid('table');
            $s3Client->getObject(array(
                'Bucket' => $fileInfo['s3Path']['bucket'],
                'Key' => $fileInfo['s3Path']['key'],
                'SaveAs' => $tmpFilePath
            ));

            $fh = fopen($tmpFilePath, 'r');
            if ($fh) {
                $this->s3Client->putObject([
                    'Bucket' => $targetBucket,
                    'Key' => $targetBasePath . str_replace('.', '/', $tableId) . '.csv.gz',
                    'Body' => $fh,
                ]);
                fclose($fh);
            } else {
                throw new \Exception(sprintf('Cannot open file %s', $tmpFilePath));
            }
            $fs->remove($tmpFilePath);
        }
    }

    public function backupConfigs(
        string $targetBucket,
        ?string $targetBasePath = null,
        bool $includeVersions = true
    ): void {
        $targetBasePath = $this->trimTargetBasePath($targetBasePath);
        $this->logger->info('Exporting configurations');

        $tmp = new Temp();
        $tmp->initRunFolder();

        $configurationsFile = $tmp->createFile('configurations.json');
        $versionsFile = $tmp->createFile('versions.json');

        // use raw api call to prevent parsing json - preserve empty JSON objects
        $this->sapiClient->apiGet('storage/components?include=configuration', $configurationsFile->getPathname());
        $handle = fopen((string) $configurationsFile, 'r');
        if ($handle) {
            $this->s3Client->putObject([
                'Bucket' => $targetBucket,
                'Key' => $targetBasePath . 'configurations.json',
                'Body' => $handle,
            ]);
            fclose($handle);
        } else {
            throw new \Exception(sprintf('Cannot open file %s', (string) $configurationsFile));
        }

        $url = 'storage/components';
        $url .= '?include=configuration,rows,state';
        $this->sapiClient->apiGet($url, $configurationsFile->getPathname());
        $configurations = json_decode((string) file_get_contents($configurationsFile->getPathname()));

        $limit = self::CONFIGURATION_PAGING_LIMIT;

        foreach ($configurations as $component) {
            $this->logger->info(sprintf('Exporting %s configurations', $component->id));

            foreach ($component->configurations as $configuration) {
                if ($includeVersions) {
                    $offset = 0;
                    $versions = [];
                    do {
                        $url = "storage/components/{$component->id}/configs/{$configuration->id}/versions";
                        $url .= '?include=name,description,configuration,state';
                        $url .= "&limit={$limit}&offset={$offset}";
                        $this->sapiClient->apiGet($url, $versionsFile->getPathname());
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
                            $url = "storage/components/{$component->id}";
                            $url .= "/configs/{$configuration->id}";
                            $url .= "/rows/{$row->id}/versions";
                            $url .= '?include=configuration';
                            $url .= "&limit={$limit}&offset={$offset}";
                            $this->sapiClient->apiGet($url, $versionsFile->getPathname());
                            $versionsTmp = json_decode((string) file_get_contents($versionsFile->getPathname()));
                            $versions = array_merge($versions, $versionsTmp);
                            $offset = $offset + $limit;
                        } while (count($versionsTmp) > 0);
                        $row->_versions = $versions;
                    }
                }
                $this->s3Client->putObject([
                    'Bucket' => $targetBucket,
                    'Key' => $targetBasePath . 'configurations/' . $component->id . '/' .
                        $configuration->id . '.json',
                    'Body' => json_encode($configuration),
                ]);
            }
        }
    }
}
