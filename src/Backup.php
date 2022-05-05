<?php

declare(strict_types=1);

namespace Keboola\ProjectBackup;

use Exception;
use Keboola\ProjectBackup\Exception\SkipTableException;
use Keboola\ProjectBackup\FileClient\AbsFileClient;
use Keboola\ProjectBackup\FileClient\IFileClient;
use Keboola\ProjectBackup\FileClient\S3FileClient;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\HandlerStack;
use Keboola\StorageApi\Options\Components\ListConfigurationMetadataOptions;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

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
            ]
        );
    }

    /**
     * @param string|resource $content
     */
    abstract protected function putToStorage(string $name, $content): void;

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
            $client = new \GuzzleHttp\Client([
                'handler' => HandlerStack::create([
                    'backoffMaxTries' => 10,
                ]),
            ]);
            $manifest = json_decode($client->get($fileInfo['url'])->getBody()->getContents(), true);

            foreach ($manifest['entries'] as $i => $part) {
                $this->putToStorage(
                    sprintf(
                        '%s.part_%d.csv.gz',
                        str_replace('.', '/', $tableId),
                        $i
                    ),
                    $fileClient->getFileContent($part)
                );
            }
        } else {
            $this->putToStorage(
                str_replace('.', '/', $tableId) . '.csv.gz',
                $fileClient->getFileContent()
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
            (new GetFileOptions())->setFederationToken(true)
        );
    }

    public function backupTablesMetadata(): void
    {
        $this->logger->info('Exporting buckets');

        $this->putToStorage(
            'buckets.json',
            (string) json_encode($this->sapiClient->listBuckets(['include' => 'attributes,metadata']))
        );

        $this->logger->info('Exporting tables');
        $tables = $this->sapiClient->listTables(null, [
            'include' => 'attributes,columns,buckets,metadata,columnMetadata',
        ]);

        $this->putToStorage('tables.json', (string) json_encode($tables));
    }

    public function backupConfigs(bool $includeVersions = true): void
    {
        $this->logger->info('Exporting configurations');

        $tmp = new Temp();
        $tmp->initRunFolder();

        $configurationsFile = $tmp->createFile('configurations.json');

        // use raw api call to prevent parsing json - preserve empty JSON objects
        $this->sapiClient->apiGet('components?include=configuration', $configurationsFile->getPathname());
        $handle = fopen((string) $configurationsFile, 'r');
        if ($handle) {
            $this->putToStorage('configurations.json', '{}');
            fclose($handle);
        } else {
            throw new Exception(sprintf('Cannot open file %s', (string) $configurationsFile));
        }
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
