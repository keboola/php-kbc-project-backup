<?php

declare(strict_types=1);

namespace Keboola\ProjectBackup\FileClient;

use Exception;
use Google\Auth\FetchAuthTokenInterface;
use Google\Cloud\Storage\StorageClient as GoogleStorageClient;
use Keboola\Temp\Temp;

class GcsFileClient implements IFileClient
{
    private Temp $tmp;

    private array $gcsPath;

    private string $tmpFilePath;

    private GoogleStorageClient $gcsClient;

    public function __construct(array $fileInfo)
    {
        $options = [
            'credentials' => [
                'access_token' => $fileInfo['gcsCredentials']['access_token'],
                'expires_in' => $fileInfo['gcsCredentials']['expires_in'],
                'token_type' => $fileInfo['gcsCredentials']['token_type'],
            ],
            'projectId' => $fileInfo['gcsCredentials']['projectId'],
        ];
        $fetchAuthToken = $this->getAuthTokenClass($options['credentials']);
        $this->gcsClient = new GoogleStorageClient([
            'projectId' => $options['projectId'],
            'credentialsFetcher' => $fetchAuthToken,
        ]);
        $this->gcsPath = $fileInfo['gcsPath'];
        $this->tmp = new Temp();
        $this->tmpFilePath = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . uniqid('sapi-export-');
    }

    /** @return resource */
    public function getFileContent(?array $filePart = null)
    {
        if ($filePart) {
            $fileKey = substr($filePart['url'], strpos($filePart['url'], '/', 5) + 1);
            $filePath = sprintf(
                '%s_%s',
                $this->tmpFilePath,
                md5(str_replace('/', '_', $fileKey)),
            );
        } else {
            $fileKey = $this->gcsPath['key'];
            $filePath = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . uniqid('table');
        }

        $retBucket = $this->gcsClient->bucket($this->gcsPath['bucket']);
        $object = $retBucket->object($fileKey);
        $object->downloadToFile($filePath);

        $file = fopen($filePath, 'r');
        if (!$file) {
            throw new Exception(sprintf('Cannot open file %s', $filePath));
        }
        return $file;
    }

    private function getAuthTokenClass(array $credentials): FetchAuthTokenInterface
    {
        return new class ($credentials) implements FetchAuthTokenInterface {
            private array $creds;

            public function __construct(
                array $creds,
            ) {
                $this->creds = $creds;
            }

            public function fetchAuthToken(?callable $httpHandler = null): array
            {
                return $this->creds;
            }

            public function getCacheKey(): string
            {
                return '';
            }

            public function getLastReceivedToken(): array
            {
                return $this->creds;
            }
        };
    }
}
