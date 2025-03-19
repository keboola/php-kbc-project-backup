<?php

declare(strict_types=1);

namespace Keboola\ProjectBackup\Tests;

use Keboola\ProjectBackup\AbsBackup;
use Keboola\ProjectBackup\Backup;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\Blob;
use MicrosoftAzure\Storage\Blob\Models\Container;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;
use MicrosoftAzure\Storage\Common\Middlewares\RetryMiddlewareFactory;

class AbsBackupTest extends AbstractTestBase
{
    private BlobRestProxy $absClient;

    public function setUp(): void
    {
        parent::setUp();

        $this->absClient = BlobRestProxy::createBlobService(sprintf(
            'DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net',
            (string) getenv('TEST_AZURE_ACCOUNT_NAME'),
            (string) getenv('TEST_AZURE_ACCOUNT_KEY'),
        ));
        $this->absClient->pushMiddleware(
            RetryMiddlewareFactory::create(),
        );

        $containers = $this->absClient->listContainers();
        $listContainers = array_map(fn(Container $v) => $v->getName(), $containers->getContainers());

        if (!in_array((string) getenv('TEST_AZURE_CONTAINER_NAME'), $listContainers)) {
            $this->absClient->createContainer((string) getenv('TEST_AZURE_CONTAINER_NAME'));
        }
        $this->cleanupAbs();
    }

    protected function getClient(): Backup
    {
        return new AbsBackup(
            $this->sapiClient,
            $this->absClient,
            (string) getenv('TEST_AZURE_CONTAINER_NAME'),
        );
    }

    protected function getCloudPrefix(): string
    {
        return 'AZURE';
    }

    protected function getStorageContent(string $path): string
    {
        $targetContents = $this->absClient->getBlob(
            (string) getenv('TEST_AZURE_CONTAINER_NAME'),
            $path,
        );
        return (string) stream_get_contents($targetContents->getContentStream());
    }

    protected function listStorageObjects(): array
    {
        $options = new ListBlobsOptions();
        $options->setPrefix('');
        $blobs = $this->absClient->listBlobs((string) getenv('TEST_AZURE_CONTAINER_NAME'), $options);
        return array_map(fn(Blob $v) => $v->getName(), $blobs->getBlobs());
    }

    private function cleanupAbs(): void
    {
        $options = new ListBlobsOptions();
        $options->setPrefix('');
        $blobs = $this->absClient->listBlobs((string) getenv('TEST_AZURE_CONTAINER_NAME'), $options);

        foreach ($blobs->getBlobs() as $blob) {
            $this->absClient->deleteBlob((string) getenv('TEST_AZURE_CONTAINER_NAME'), $blob->getName());
        }
    }
}
