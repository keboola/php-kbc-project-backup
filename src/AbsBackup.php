<?php

declare(strict_types=1);

namespace Keboola\ProjectBackup;

use GuzzleHttp\Client;
use Keboola\ProjectBackup\Exception\SkipTableException;
use Keboola\StorageApi\Client as StorageApi;
use Keboola\StorageApi\HandlerStack;
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

    public function backupTable(string $tableId): void
    {
        try {
            $fileInfo = $this->getTableFileInfo($tableId);
        } catch (SkipTableException $e) {
            return;
        }

        // Initialize BlobRestProxy with credentials from Storage API
        $absClient = BlobRestProxy::createBlobService($fileInfo['absCredentials']['SASConnectionString']);

        if ($fileInfo['isSliced'] === true) {
            // Download manifest with all sliced files
            $client = new Client([
                'handler' => HandlerStack::create([
                    'backoffMaxTries' => 10,
                ]),
            ]);
            $manifest = json_decode($client->get($fileInfo['url'])->getBody()->getContents(), true);

            // Download all slices
            foreach ($manifest['entries'] as $i => $part) {
                $partFileInfo = new \SplFileInfo($part['url']);
                $fileContent = $absClient->getBlob($fileInfo['absPath']['container'], $partFileInfo->getFilename());

                $this->putToStorage(
                    str_replace('.', '/', $tableId) . '.part_' . $i . '.csv.gz',
                    $fileContent->getContentStream()
                );
            }
        } else {
            $fileContent = $absClient->getBlob($fileInfo['absPath']['container'], $fileInfo['absPath']['name']);
            $this->putToStorage(
                str_replace('.', '/', $tableId) . '.csv.gz',
                $fileContent->getContentStream()
            );
        }
    }
}
