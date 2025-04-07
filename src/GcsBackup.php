<?php

declare(strict_types=1);

namespace Keboola\ProjectBackup;

use DateTimeImmutable;
use Google\Cloud\Storage\StorageClient;
use Google\Cloud\Storage\StorageObject;
use GuzzleHttp\Exception\RequestException;
use Keboola\StorageApi\Client as StorageApi;
use Psr\Log\LoggerInterface;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\CallableRetryPolicy;
use Retry\RetryProxy;
use Throwable;

class GcsBackup extends Backup
{
    // see https://cloud.google.com/storage/docs/retry-strategy
    public const RETRY_HTTP_CODES = [
        408, // Request Timeout
        429, // Too Many Requests
        500, // Internal Server Error
        502, // Bad Gateway
        503, // Service Unavailable
        504, // Gateway Timeout
    ];

    public const MAX_RETRIES = 3;

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

        /** @var StorageObject $object */
        $object = $this->createRetry()->call(
            fn() => $bucket->upload($content, [
                'name' => sprintf('%s%s', $this->path, $name),
            ]),
        );

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

    private function createRetry(): RetryProxy
    {
        $backOffPolicy = new ExponentialBackOffPolicy(1000);
        $retryPolicy = new CallableRetryPolicy(function (Throwable $e) {
            if ($e instanceof RequestException && in_array($e->getCode(), self::RETRY_HTTP_CODES, true)) {
                return true;
            }

            return false;
        }, self::MAX_RETRIES);

        return new RetryProxy($retryPolicy, $backOffPolicy, $this->logger);
    }
}
