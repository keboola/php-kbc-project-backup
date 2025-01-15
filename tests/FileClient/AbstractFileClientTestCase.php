<?php

declare(strict_types=1);

namespace Keboola\ProjectBackup\Tests\FileClient;

use Keboola\Csv\CsvFile;
use Keboola\ProjectBackup\FileClient\IFileClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\GetFileOptions;
use PHPUnit\Framework\TestCase;

abstract class AbstractFileClientTestCase extends TestCase
{
    private Client $storageClient;

    abstract protected function createFileClient(array $fileInfo): IFileClient;

    abstract protected function initStorageClient(): Client;

    public function setUp(): void
    {
        $this->storageClient = $this->initStorageClient();
        parent::setUp();
    }

    public function testDownloadFile(): void
    {
        $tableId = 'out.c-testDownloadData.testTable';
        $this->createTableInStorage($tableId);

        $fileInfo = $this->getFileInfo($tableId);
        $fileClient = $this->createFileClient($fileInfo);

        if ($fileInfo['isSliced']) {
            /** @var array{
             *     entries: array,
             * } $manifest
             */
            $manifest = json_decode((string) file_get_contents($fileInfo['url']), true);

            // try only first part
            $file = $fileClient->getFileContent((array) $manifest['entries'][0]);
        } else {
            $file = $fileClient->getFileContent();
        }

        // compressed file
        self::assertNotEmpty(fread($file, 100));
    }

    protected function createTableInStorage(string $tableId): void
    {
        list($stage, $bucket, $table) = explode('.', $tableId);
        $bucketId = sprintf('%s.%s', $stage, $bucket);
        $tableId = sprintf('%s.%s', $bucketId, $table);

        if (!$this->storageClient->bucketExists($bucketId)) {
            $this->storageClient->createBucket($bucket, $stage);
        }
        if ($this->storageClient->tableExists($tableId)) {
            $this->storageClient->dropTable($tableId);
        }

        $this->storageClient->createTableAsync(
            $bucketId,
            $table,
            new CsvFile(__DIR__ . '/../data/sample.csv'),
        );
    }

    protected function getFileInfo(string $tableId): array
    {
        $fileId = $this->storageClient->exportTableAsync($tableId, [
            'gzip' => true,
        ]);

        return (array) $this->storageClient->getFile(
            $fileId['file']['id'],
            (new GetFileOptions())->setFederationToken(true),
        );
    }
}
