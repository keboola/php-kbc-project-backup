<?php

declare(strict_types=1);

namespace Keboola\ProjectBackup\Tests;

use Google\Cloud\Storage\Bucket;
use Google\Cloud\Storage\StorageClient;
use Google\Cloud\Storage\StorageObject;
use GuzzleHttp\Exception\ClientException;
use GuzzleHttp\Exception\ServerException;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\ProjectBackup\Backup;
use Keboola\ProjectBackup\GcsBackup;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use ReflectionMethod;

class GcsBackupTest extends AbstractTestBase
{
    private StorageClient $storageClient;

    public function setUp(): void
    {
        parent::setUp();

        $this->storageClient = new StorageClient([
            'keyFile' => json_decode((string) getenv('TEST_GCP_SERVICE_ACCOUNT'), true),
        ]);

        $this->cleanupGCS();
    }

    protected function getClient(): Backup
    {
        return new GcsBackup(
            $this->sapiClient,
            $this->storageClient,
            (string) getenv('TEST_GCP_BUCKET'),
            'backup/',
            true,
        );
    }

    protected function getCloudPrefix(): string
    {
        return 'GCP';
    }

    protected function getStorageContent(string $path): string
    {
        return $this->storageClient
            ->bucket((string) getenv('TEST_GCP_BUCKET'))
            ->object('backup/' . $path)
            ->downloadAsString();
    }

    protected function listStorageObjects(): array
    {
        return array_map(
            fn(StorageObject $object) => str_replace('backup/', '', $object->name()),
            iterator_to_array($this->storageClient->bucket((string) getenv('TEST_GCP_BUCKET'))->objects()),
        );
    }

    public function testBackupSignedUrls(): void
    {
        $component = new Components($this->branchAwareClient);

        $config = new Configuration();
        $config->setComponentId('keboola.snowflake-transformation');
        $config->setDescription('Test Configuration');
        $config->setConfigurationId('sapi-php-test');
        $config->setName('test-configuration');
        $configData = $component->addConfiguration($config);
        $config->setConfigurationId($configData['id']);

        $row = new ConfigurationRow($config);
        $row->setChangeDescription('Row 1');
        $row->setConfiguration(
            ['name' => 'test 1', 'backend' => 'docker', 'type' => 'r', 'queries' => ['foo']],
        );
        $component->addConfigurationRow($row);

        $backup = new GcsBackup(
            $this->sapiClient,
            $this->storageClient,
            (string) getenv('TEST_GCP_BUCKET'),
            'backup/',
            true,
        );
        $backup->backupConfigs(false);
        $backup->backupSignedUrls();

        $targetContents = $this->storageClient
            ->bucket((string) getenv('TEST_GCP_BUCKET'))
            ->object('backup/signedUrls.json')
            ->downloadAsString();

        /** @var array $targetData */
        $targetData = json_decode($targetContents, true);

        self::assertArrayHasKey('configurations.json', $targetData);
        self::assertArrayHasKey('configurations', $targetData);
        self::assertArrayHasKey('keboola.snowflake-transformation', $targetData['configurations']);
        self::assertArrayHasKey(
            'sapi-php-test.json',
            $targetData['configurations']['keboola.snowflake-transformation'],
        );

        self::assertTrue(str_contains(
            $targetData['configurations']['keboola.snowflake-transformation']['sapi-php-test.json'],
            'https://storage.googleapis.com/' . (string) getenv('TEST_GCP_BUCKET') . '/backup/',
        ));
    }

    private function cleanupGCS(): void
    {
        $objects = $this->storageClient->bucket((string) getenv('TEST_GCP_BUCKET'))->objects();

        /** @var StorageObject $object */
        foreach ($objects as $object) {
            $object->delete();
        }
    }

    /**
     * Test that upload retries on temporary failures and succeeds
     */
    public function testUploadRetriesOnTemporaryFailure(): void
    {
        $mockStorageClient = $this->createMock(StorageClient::class);
        $mockBucket = $this->createMock(Bucket::class);

        $mockStorageClient->method('bucket')
            ->willReturn($mockBucket);

        // Configure the mock to fail twice with 503 then succeed
        $mockBucket->expects($this->exactly(3))
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
                $this->createMock(StorageObject::class), // successful response
            );

        $backup = new GcsBackup(
            $this->sapiClient,
            $mockStorageClient,
            'test-bucket',
            'test-path',
            true,
        );

        // Use reflection to access protected method
        $method = new ReflectionMethod(GcsBackup::class, 'putToStorage');
        $method->setAccessible(true);

        // Should not throw exception as it should eventually succeed
        $method->invoke($backup, 'test.json', 'test-content');
    }

    /**
     * Test that upload fails after maximum retries
     */
    public function testUploadFailsAfterMaxRetries(): void
    {
        $mockStorageClient = $this->createMock(StorageClient::class);
        $mockBucket = $this->createMock(Bucket::class);

        $mockStorageClient->method('bucket')
            ->willReturn($mockBucket);

        // Configure the mock to fail with 503 more times than max retries
        $mockBucket->expects($this->exactly(GcsBackup::MAX_RETRIES))
            ->method('upload')
            ->willThrowException(new ServerException(
                'Service Unavailable',
                new Request('PUT', 'test-url'),
                new Response(503),
            ));

        $backup = new GcsBackup(
            $this->sapiClient,
            $mockStorageClient,
            'test-bucket',
            'test-path',
            true,
        );

        // Use reflection to access protected method
        $method = new ReflectionMethod(GcsBackup::class, 'putToStorage');
        $method->setAccessible(true);

        $this->expectException(ServerException::class);
        $method->invoke($backup, 'test.json', 'test-content');
    }

    /**
     * Test that non-retryable errors fail immediately
     */
    public function testNonRetryableErrorFailsImmediately(): void
    {
        $mockStorageClient = $this->createMock(StorageClient::class);
        $mockBucket = $this->createMock(Bucket::class);

        $mockStorageClient->method('bucket')
            ->willReturn($mockBucket);

        // Configure the mock to fail with 400 (non-retryable)
        $mockBucket->expects($this->exactly(1))
            ->method('upload')
            ->willThrowException(new ClientException(
                'Bad Request',
                new Request('PUT', 'test-url'),
                new Response(400),
            ));

        $backup = new GcsBackup(
            $this->sapiClient,
            $mockStorageClient,
            'test-bucket',
            'test-path',
            true,
        );

        // Use reflection to access protected method
        $method = new ReflectionMethod(GcsBackup::class, 'putToStorage');
        $method->setAccessible(true);

        $this->expectException(ClientException::class);
        $method->invoke($backup, 'test.json', 'test-content');
    }
}
