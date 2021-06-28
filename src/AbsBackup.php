<?php

declare(strict_types=1);

namespace Keboola\ProjectBackup;

use Keboola\StorageApi\Client as StorageApi;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use Psr\Log\LoggerInterface;

class AbsBackup extends Backup
{
    private BlobRestProxy $absClient;

    private string $path;

    public function __construct(
        StorageApi $sapiClient,
        BlobRestProxy $absClient,
        string $path,
        ?LoggerInterface $logger = null
    ) {
        $this->absClient = $absClient;
        $this->path = $path;

        parent::__construct($sapiClient, $logger);
    }

    /**
     * @param resource|string $content
     */
    protected function putToStorage(string $name, $content): void
    {
        $this->absClient->createBlockBlob(
            $this->path,
            $name,
            $content
        );
    }
}
