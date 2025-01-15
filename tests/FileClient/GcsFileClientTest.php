<?php

declare(strict_types=1);

namespace Keboola\ProjectBackup\Tests\FileClient;

use Keboola\ProjectBackup\FileClient\GcsFileClient;
use Keboola\ProjectBackup\FileClient\IFileClient;
use Keboola\StorageApi\Client;

class GcsFileClientTest extends AbstractFileClientTestCase
{
    protected function createFileClient(array $fileInfo): IFileClient
    {
        return new GcsFileClient($fileInfo);
    }

    protected function initStorageClient(): Client
    {
        return new Client([
            'token' => getenv('TEST_GCP_STORAGE_API_TOKEN'),
            'url' => getenv('TEST_GCP_STORAGE_API_URL'),
        ]);
    }
}
