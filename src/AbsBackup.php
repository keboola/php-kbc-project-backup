<?php

declare(strict_types=1);

namespace Keboola\ProjectBackup;

use GuzzleHttp\Client;
use Keboola\StorageApi\Client as StorageApi;
use Keboola\StorageApi\HandlerStack;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\Temp\Temp;
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

    protected function putToStorage(string $name, string $content): void
    {
        $this->absClient->createBlockBlob(
            $this->path,
            $name,
            $content
        );
    }

    public function backupTable(string $tableId): void
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

        $this->logger->info(sprintf('Exporting table %s', $tableId));

        $tmp = new Temp();
        $tmp->initRunFolder();

        $fileId = $this->sapiClient->exportTableAsync($tableId, [
            'gzip' => true,
        ]);
        $fileInfo = $this->sapiClient->getFile($fileId['file']['id'], (new GetFileOptions())->setFederationToken(true));

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

                $this->absClient->createBlockBlob(
                    $this->path,
                    str_replace('.', '/', $tableId) . '.part_' . $i . '.csv.gz',
                    (string) stream_get_contents($fileContent->getContentStream())
                );
            }
        } else {
            $fileContent = $absClient->getBlob($fileInfo['absPath']['container'], $fileInfo['absPath']['name']);
            $this->absClient->createBlockBlob(
                $this->path,
                str_replace('.', '/', $tableId) . '.csv.gz',
                (string) stream_get_contents($fileContent->getContentStream())
            );
        }
    }
}
