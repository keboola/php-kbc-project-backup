<?php

declare(strict_types=1);

namespace Keboola\ProjectBackup;

use Aws\S3\S3Client;
use GuzzleHttp\Exception\RequestException;
use Keboola\StorageApi\Client as StorageApi;
use Psr\Log\LoggerInterface;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\CallableRetryPolicy;
use Retry\RetryProxy;
use Throwable;

class S3Backup extends Backup
{
    public const MAX_RETRIES = 3;
    public const RETRY_HTTP_CODES = [
        500, // 500 Internal Serve Error
        502, // 502 Bad Gateway
        503, // 503 Service Unavailable
        504, // 504 Gateway Timeout
    ];

    private S3Client $s3Client;

    private string $bucket;

    private string $path;

    public function __construct(
        StorageApi $sapiClient,
        S3Client $s3Client,
        string $bucket,
        string $path,
        ?LoggerInterface $logger = null,
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
        $this->createRetry()->call(
            fn() => $this->s3Client->upload(
                $this->bucket,
                $this->path . $name,
                $content,
            ),
        );
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
