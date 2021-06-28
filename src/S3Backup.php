<?php

declare(strict_types=1);

namespace Keboola\ProjectBackup;

use Aws\S3\S3Client;
use Keboola\StorageApi\Client as StorageApi;
use Psr\Log\LoggerInterface;

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
        $this->s3Client->upload(
            $this->bucket,
            $this->path . $name,
            $content
        );
    }
}
