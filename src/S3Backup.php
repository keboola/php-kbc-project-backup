<?php

declare(strict_types=1);

namespace Keboola\ProjectBackup;

use Aws\S3\S3Client;
use Keboola\ProjectBackup\Exception\SkipTableException;
use Keboola\StorageApi\Client as StorageApi;
use Keboola\StorageApi\HandlerStack;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
use Symfony\Component\Filesystem\Filesystem;

class S3Backup extends Backup
{

    private S3Client $s3Client;

    private string $bucket;

    private string $path;

    public function __construct(
        StorageApi $sapiClient,
        S3Client $s3Client,
        string $bucket,
        string $path,
        ?LoggerInterface $logger = null
    ) {
        $this->bucket = $bucket;
        $this->s3Client = $s3Client;
        $this->path = $this->trimTargetBasePath($path);

        parent::__construct($sapiClient, $logger);
    }

    private function trimTargetBasePath(?string $targetBasePath = null): string
    {
        if (empty($targetBasePath) || $targetBasePath === '/') {
            return '';
        } else {
            return trim($targetBasePath, '/') . '/';
        }
    }

    /**
     * @param resource|string $content
     */
    protected function putToStorage(string $name, $content): void
    {
        $this->s3Client->putObject([
            'Bucket' => $this->bucket,
            'Key' => $this->path . $name,
            'Body' => $content,
        ]);
    }

    public function backupTable(string $tableId): void
    {
        try {
            $fileInfo = $this->getTableFileInfo($tableId);
        } catch (SkipTableException $e) {
            return;
        }

        $tmp = new Temp();
        $tmp->initRunFolder();

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
                        'Bucket' => $this->bucket,
                        'Key' => $this->path . str_replace('.', '/', $tableId) . '.part_' . $i . '.csv.gz',
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
                $this->putToStorage(
                    $this->path . str_replace('.', '/', $tableId) . '.csv.gz',
                    $fh
                );
                fclose($fh);
            } else {
                throw new \Exception(sprintf('Cannot open file %s', $tmpFilePath));
            }
            $fs->remove($tmpFilePath);
        }
    }
}
