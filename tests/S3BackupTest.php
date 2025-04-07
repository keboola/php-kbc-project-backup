<?php

declare(strict_types=1);

namespace Keboola\ProjectBackup\Tests;

use Aws\S3\S3Client;
use GuzzleHttp\Exception\ClientException;
use GuzzleHttp\Exception\ServerException;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\ProjectBackup\Backup;
use Keboola\ProjectBackup\S3Backup;
use Keboola\Temp\Temp;
use ReflectionMethod;

class S3BackupTest extends AbstractTestBase
{
    private S3Client $s3Client;

    public function setUp(): void
    {
        parent::setUp();

        putenv('AWS_ACCESS_KEY_ID=' . getenv('TEST_AWS_ACCESS_KEY_ID'));
        putenv('AWS_SECRET_ACCESS_KEY=' . getenv('TEST_AWS_SECRET_ACCESS_KEY'));

        $this->s3Client = new S3Client([
            'version' => 'latest',
            'region' => getenv('TEST_AWS_REGION'),
        ]);

        $this->cleanupS3();
    }

    protected function getClient(): Backup
    {
        return new S3Backup(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'backup',
        );
    }

    protected function getCloudPrefix(): string
    {
        return 'AWS';
    }

    protected function getStorageContent(string $path): string
    {
        $temp = new Temp();
        $targetFile = $temp->createTmpFile($path);
        $this->s3Client->getObject([
            'Bucket' => getenv('TEST_AWS_S3_BUCKET'),
            'Key' => 'backup/' . $path,
            'SaveAs' => (string) $targetFile,
        ]);
        return (string) file_get_contents((string) $targetFile);
    }

    protected function listStorageObjects(): array
    {
        $keys = $this->s3Client->listObjects(['Bucket' => getenv('TEST_AWS_S3_BUCKET')])->toArray();
        if (isset($keys['Contents'])) {
            $keys = $keys['Contents'];
        } else {
            $keys = [];
        }

        return array_map(function ($key) {
            return str_replace('backup/', '', $key['Key']);
        }, $keys);
    }

    /**
     * Test that upload retries on temporary failures and succeeds
     */
    public function testUploadRetriesOnTemporaryFailure(): void
    {
        $mockS3Client = $this->createMock(S3Client::class);

        // Configure the mock to fail twice with 503 then succeed
        $mockS3Client->expects($this->exactly(3))
            ->method('upload')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new ServerException(
                    'Service Unavailable',
                    new Request('PUT', 'test-url'),
                    new Response(503),
                )),
                $this->throwException(new ServerException(
                    'Service Unavailable',
                    new Request('PUT', 'test-url'),
                    new Response(503),
                )),
                ['ETag' => 'test-etag'], // successful response
            );

        $backup = new S3Backup(
            $this->sapiClient,
            $mockS3Client,
            'test-bucket',
            'test-path',
        );

        // Use reflection to access protected method
        $method = new ReflectionMethod(S3Backup::class, 'putToStorage');
        $method->setAccessible(true);

        // Should not throw exception as it should eventually succeed
        $method->invoke($backup, 'test.json', 'test-content');
    }

    /**
     * Test that upload fails after maximum retries
     */
    public function testUploadFailsAfterMaxRetries(): void
    {
        $mockS3Client = $this->createMock(S3Client::class);

        // Configure the mock to fail with 503 more times than max retries
        $mockS3Client->expects($this->exactly(S3Backup::MAX_RETRIES))
            ->method('upload')
            ->willThrowException(new ServerException(
                'Service Unavailable',
                new Request('PUT', 'test-url'),
                new Response(503),
            ));

        $backup = new S3Backup(
            $this->sapiClient,
            $mockS3Client,
            'test-bucket',
            'test-path',
        );

        // Use reflection to access protected method
        $method = new ReflectionMethod(S3Backup::class, 'putToStorage');
        $method->setAccessible(true);

        $this->expectException(ServerException::class);
        $method->invoke($backup, 'test.json', 'test-content');
    }

    /**
     * Test that non-retryable errors fail immediately
     */
    public function testNonRetryableErrorFailsImmediately(): void
    {
        $mockS3Client = $this->createMock(S3Client::class);

        // Configure the mock to fail with 400 (non-retryable)
        $mockS3Client->expects($this->exactly(1))
            ->method('upload')
            ->willThrowException(new ClientException(
                'Bad Request',
                new Request('PUT', 'test-url'),
                new Response(400),
            ));

        $backup = new S3Backup(
            $this->sapiClient,
            $mockS3Client,
            'test-bucket',
            'test-path',
        );

        // Use reflection to access protected method
        $method = new ReflectionMethod(S3Backup::class, 'putToStorage');
        $method->setAccessible(true);

        $this->expectException(ClientException::class);
        $method->invoke($backup, 'test.json', 'test-content');
    }

    private function cleanupS3(): void
    {
        $keys = $this->s3Client->listObjects(['Bucket' => getenv('TEST_AWS_S3_BUCKET')])->toArray();
        if (isset($keys['Contents'])) {
            $keys = $keys['Contents'];
        } else {
            $keys = [];
        }

        $deleteObjects = [];
        foreach ($keys as $key) {
            $deleteObjects[] = $key;
        }

        foreach ($deleteObjects as $deleteObject) {
            $this->s3Client->deleteObject([
                'Bucket' => getenv('TEST_AWS_S3_BUCKET'),
                'Key' => $deleteObject['Key'],
            ]);
        }
    }
}
