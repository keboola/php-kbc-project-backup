<?php
namespace Keboola\StorageApi\Project;

use Aws\S3\S3Client;
use Keboola\StorageApi\Client AS StorageApi;
use Keboola\StorageApi\HandlerStack;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Symfony\Component\Filesystem\Filesystem;

class BackupS3
{
    private const CONFIGS_VERSION_LIMIT = 2;

    /**
     * @var StorageApi
     */
    private $sapiClient;

    /**
     * @var S3Client
     */
    private $s3Client;

    /**
     * @var NullLogger
     */
    private $logger;

    public function __construct(StorageApi $sapiClient, S3Client $s3Client, LoggerInterface $logger = null)
    {
        $this->sapiClient = $sapiClient;
        $this->s3Client = $s3Client;
        $this->logger = $logger?: new NullLogger();
    }

    private function trimTargetBasePath(string $targetBasePath = null)
    {
        if (empty($targetBasePath) || $targetBasePath === '/') {
            return '';
        } else {
            return rtrim($targetBasePath, '/') . '/';
        }
    }

    public function backup(string $targetBucket, string $targetBasePath = null): void
    {
        $onlyStructure = false;
        $includeVersions = true;

        //@FIXME backup methods cannot return void

        $tables = $this->backupTablesMetadata($targetBucket, $targetBasePath);
        usort($tables, function ($a, $b) {
            return strcmp($a["id"], $b["id"]);
        });

        //@FIXME resolve counter? (backup stats?) custom line
        foreach (array_values($tables) as $i => $table) {
            if ($onlyStructure && $table['bucket']['stage'] !== 'sys') {
                $this->logger->warning(sprintf('Skipping table %s (sys bucket)', $table['id']));
            } elseif (!$table['isAlias']) {
                $this->backupTable($table['id'], $targetBucket, $targetBasePath);
            } else {
                $this->logger->warning(sprintf('Skipping table %s (alias)', $table['id']));
            }
        }

        $this->backupConfigs($targetBucket, $targetBasePath, $includeVersions);
    }


    public function backupTablesMetadata(string $targetBucket, string $targetBasePath = null): array
    {
        $targetBasePath = $this->trimTargetBasePath($targetBasePath);
        $this->logger->info('Exporting buckets');

        $this->s3Client->putObject([
            'Bucket' => $targetBucket,
            'Key' => $targetBasePath . 'buckets.json',
            'Body' => json_encode($this->sapiClient->listBuckets(["include" => "attributes,metadata"])),
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

        return $tables;
        return array_map(function ($row) { return $row['id']; }, $tables);
    }

    public function backupTable(string $tableId, string $targetBucket, string $targetBasePath = null): void
    {
        $targetBasePath = $this->trimTargetBasePath($targetBasePath);
        $this->logger->info(sprintf('Exporting table %s', $tableId));

        $tmp = new Temp();
        $tmp->initRunFolder();

        $fileId = $this->sapiClient->exportTableAsync($tableId, [
            'gzip' => true,
        ]);
        $fileInfo = $this->sapiClient->getFile($fileId["file"]["id"], (new GetFileOptions())->setFederationToken(true));

        $this->logger->info('region' . $fileInfo["region"]); //@FIXME remove

        // Initialize S3Client with credentials from Storage API
        $s3Client = new S3Client([
            "version" => "latest",
            "region" => $fileInfo["region"],
            "credentials" => [
                "key" => $fileInfo["credentials"]["AccessKeyId"],
                "secret" => $fileInfo["credentials"]["SecretAccessKey"],
                "token" => $fileInfo["credentials"]["SessionToken"],
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
            $manifest = json_decode($client->get($fileInfo['url'])->getBody(), true);

            // Download all slices
            //@FIXME better temps
            $tmpFilePath = $tmp->getTmpFolder() . DIRECTORY_SEPARATOR . uniqid('sapi-export-');
            foreach ($manifest["entries"] as $i => $part) {
                $fileKey = substr($part["url"], strpos($part["url"], '/', 5) + 1);
                $filePath = $tmpFilePath . '_' . md5(str_replace('/', '_', $fileKey));
                $this->s3Client->getObject(array(
                    'Bucket' => $fileInfo["s3Path"]["bucket"],
                    'Key' => $fileKey,
                    'SaveAs' => $filePath
                ));
                $fh = fopen($filePath, 'r');
                $this->s3Client->putObject([
                    'Bucket' => $targetBucket,
                    'Key' => $targetBasePath . str_replace('.', '/', $tableId) . '.part_' . $i . '.csv.gz',
                    'Body' => $fh,
                ]);
                fclose($fh);
                $fs->remove($filePath);
            }
        } else {
            $tmpFilePath = $tmp->getTmpFolder() . DIRECTORY_SEPARATOR . uniqid('table');
            $s3Client->getObject(array(
                'Bucket' => $fileInfo["s3Path"]["bucket"],
                'Key' => $fileInfo["s3Path"]["key"],
                'SaveAs' => $tmpFilePath
            ));

            $fh = fopen($tmpFilePath, 'r');
            $this->s3Client->putObject([
                'Bucket' => $targetBucket,
                'Key' => $targetBasePath . str_replace('.', '/', $tableId) . '.csv.gz',
                'Body' => $fh,
            ]);
            fclose($fh);
            $fs->remove($tmpFilePath);
        }
    }


    public function backupConfigs($targetBucket, $targetBasePath = null, $saveVersions): void
    {
        $targetBasePath = $this->trimTargetBasePath($targetBasePath);
        $limit = self::CONFIGS_VERSION_LIMIT;
        $this->logger->info('Exporting configurations');

        $tmp = new Temp();
        $tmp->initRunFolder();

        $configurationsFile = $tmp->createFile("configurations.json");
        $versionsFile = $tmp->createFile("versions.json");

        // use raw api call to prevent parsing json - preserve empty JSON objects
        $this->sapiClient->apiGet('storage/components?include=configuration', $configurationsFile->getPathname());
        $handle = fopen($configurationsFile, "r");
        $this->s3Client->putObject([
            'Bucket' => $targetBucket,
            'Key' => $targetBasePath . 'configurations.json',
            'Body' => $handle,
        ]);
        fclose($handle);

        $url = "storage/components";
        $url .= "?include=configuration,rows,state";
        $this->sapiClient->apiGet($url, $configurationsFile->getPathname());
        $configurations = json_decode(file_get_contents($configurationsFile->getPathname()));

        foreach ($configurations as $component) {
            $this->logger->info(sprintf('Exporting %s configurations', $component->id));

            foreach ($component->configurations as $configuration) {
                if ($saveVersions) {
                    $offset = 0;
                    $versions = [];
                    do {
                        $url = "storage/components/{$component->id}/configs/{$configuration->id}/versions";
                        $url .= "?include=name,description,configuration,state";
                        $url .= "&limit={$limit}&offset={$offset}";
                        $this->sapiClient->apiGet($url, $versionsFile->getPathname());
                        $versionsTmp = json_decode(file_get_contents($versionsFile->getPathname()));
                        $versions = array_merge($versions, $versionsTmp);
                        $offset = $offset + $limit;
                    } while (count($versionsTmp) > 0);
                    $configuration->_versions = $versions;
                }
                if ($saveVersions) {
                    foreach ($configuration->rows as &$row) {
                        $offset = 0;
                        $versions = [];
                        do {
                            $url = "storage/components/{$component->id}/configs/{$configuration->id}/rows/{$row->id}/versions";
                            $url .= "?include=configuration";
                            $url .= "&limit={$limit}&offset={$offset}";
                            $this->sapiClient->apiGet($url, $versionsFile->getPathname());
                            $versionsTmp = json_decode(file_get_contents($versionsFile->getPathname()));
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