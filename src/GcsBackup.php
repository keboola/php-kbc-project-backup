<?php

declare(strict_types=1);

namespace Keboola\ProjectBackup;

use DateTimeImmutable;
use Google\Cloud\Storage\StorageClient;
use Google\Cloud\Storage\StorageObject;
use Keboola\StorageApi\Client as StorageApi;
use Psr\Log\LoggerInterface;

class GcsBackup extends Backup
{
    private array $signedUrls = [];

    public function __construct(
        StorageApi $sapiClient,
        readonly StorageClient $storageClient,
        readonly string $bucketName,
        readonly string $path,
        readonly bool $generateSignedUrls = false,
        ?LoggerInterface $logger = null,
    ) {
        parent::__construct($sapiClient, $logger);
    }

    /**
     * @param resource|string $content
     */
    protected function putToStorage(string $name, $content): void
    {
        $bucket = $this->storageClient->bucket($this->bucketName);
        $object = $bucket->upload($content, [
            'name' => sprintf('%s%s', $this->path, $name),
        ]);

        if ($this->generateSignedUrls) {
            $this->buildTreeFromPath($object, $name);
        }
    }

    public function backupSignedUrls(): void
    {
        $name = 'signedUrls.json';
        $bucket = $this->storageClient->bucket($this->bucketName);
        $content = (string) json_encode($this->signedUrls, JSON_PRETTY_PRINT);

        $bucket->upload($content, [
            'name' => sprintf('%s%s', $this->path, $name),
        ]);
    }

    private function buildTreeFromPath(StorageObject $object, string $path): void
    {
        $parts = explode('/', $path);
        $filename = pathinfo(array_pop($parts), PATHINFO_BASENAME);

        $current = &$this->signedUrls;
        foreach ($parts as $part) {
            if (!isset($current[$part])) {
                $current[$part] = [];
            }
            $current = &$current[$part];
        }

        $current[$filename] = $object->signedUrl(new DateTimeImmutable('+2 days'));
    }
}
