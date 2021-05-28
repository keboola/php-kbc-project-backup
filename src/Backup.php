<?php

declare(strict_types=1);

namespace Keboola\ProjectBackup;

use Exception;
use Keboola\ProjectBackup\Exception\SkipTableException;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

abstract class Backup
{
    private const CONFIGURATION_PAGING_LIMIT = 2;

    protected LoggerInterface $logger;

    protected Client $sapiClient;

    public function __construct(Client $sapiClient, ?LoggerInterface $logger)
    {
        $this->sapiClient = $sapiClient;
        $this->logger = $logger ?: new NullLogger();
    }

    abstract protected function putToStorage(string $name, string $content): void;

    abstract public function backupTable(string $tableId): void;

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
        $versionsFile = $tmp->createFile('versions.json');

        // use raw api call to prevent parsing json - preserve empty JSON objects
        $this->sapiClient->apiGet('storage/components?include=configuration', $configurationsFile->getPathname());
        $handle = fopen((string) $configurationsFile, 'r');
        if ($handle) {
            $this->putToStorage('configurations.json', (string) stream_get_contents($handle));
            fclose($handle);
        } else {
            throw new Exception(sprintf('Cannot open file %s', (string) $configurationsFile));
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
                $this->putToStorage(
                    sprintf(
                        'configurations/%s/%s.json',
                        $component->id,
                        $configuration->id
                    ),
                    (string) json_encode($configuration)
                );
            }
        }
    }
}
