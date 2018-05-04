<?php

namespace Keboola\ProjectBackup\Tests;

use Aws\S3\S3Client;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client as StorageApi;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\ProjectBackup\S3Backup;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;

class S3BackupTest extends TestCase
{
    /**
     * @var StorageApi
     */
    private $sapiClient;

    /**
     * @var S3Client
     */
    private $s3Client;

    public function setUp(): void
    {
        parent::setUp();

        $this->sapiClient = new StorageApi([
            'url' => TEST_STORAGE_API_URL,
            'token' => TEST_STORAGE_API_TOKEN,
        ]);

        $this->cleanupKbcProject();

        putenv('AWS_ACCESS_KEY_ID=' . TEST_AWS_ACCESS_KEY_ID);
        putenv('AWS_SECRET_ACCESS_KEY=' . TEST_AWS_SECRET_ACCESS_KEY);

        $this->s3Client = new S3Client([
            'version' => 'latest',
            'region' => TEST_AWS_REGION,
        ]);

        $this->cleanupS3();
    }

    public function testExecuteNoVersions(): void
    {
        $component = new Components($this->sapiClient);

        $config = new Configuration();
        $config->setComponentId('transformation');
        $config->setDescription('Test Configuration');
        $config->setConfigurationId('sapi-php-test');
        $config->setName('test-configuration');
        $configData = $component->addConfiguration($config);
        $config->setConfigurationId($configData['id']);

        $row = new ConfigurationRow($config);
        $row->setChangeDescription('Row 1');
        $row->setConfiguration(
            ['name' => 'test 1', 'backend' => 'docker', 'type' => 'r', 'queries' => ['foo']]
        );
        $component->addConfigurationRow($row);

        $row = new ConfigurationRow($config);
        $row->setChangeDescription('Row 2');
        $row->setConfiguration(
            ['name' => 'test 2', 'backend' => 'docker', 'type' => 'r', 'queries' => ['bar']]
        );
        $component->addConfigurationRow($row);

        $backup = new S3Backup($this->sapiClient, $this->s3Client);
        $backup->backupConfigs(TEST_AWS_S3_BUCKET, 'backup', false);

        $temp = new Temp();
        $temp->initRunFolder();

        $targetFile = $temp->createTmpFile('configurations.json');
        $this->s3Client->getObject([
            'Bucket' => TEST_AWS_S3_BUCKET,
            'Key' => 'backup/configurations.json',
            'SaveAs' => (string) $targetFile,
        ]);

        $targetContents = file_get_contents($targetFile);

        $targetData = json_decode($targetContents, true);
        $targetComponent = [];
        foreach ($targetData as $component) {
            if ($component['id'] == 'transformation') {
                $targetComponent = $component;
                break;
            }
        }
        self::assertGreaterThan(0, count($targetComponent));

        $targetConfiguration = [];
        foreach ($targetComponent['configurations'] as $configuration) {
            if ($configuration['name'] == 'test-configuration') {
                $targetConfiguration = $configuration;
            }
        }
        self::assertGreaterThan(0, count($targetConfiguration));
        self::assertEquals('Test Configuration', $targetConfiguration['description']);
        self::assertArrayNotHasKey('rows', $targetConfiguration);

        $configurationId = $targetConfiguration['id'];
        $targetFile = $temp->createTmpFile('configurations.json');
        $this->s3Client->getObject([
            'Bucket' => TEST_AWS_S3_BUCKET,
            'Key' => 'backup/configurations/transformation/' . $configurationId . '.json',
            'SaveAs' => (string) $targetFile,
        ]);
        $targetContents = file_get_contents($targetFile);
        $targetConfiguration = json_decode($targetContents, true);

        self::assertGreaterThan(0, count($targetConfiguration));
        self::assertEquals('test-configuration', $targetConfiguration['name']);
        self::assertEquals('Test Configuration', $targetConfiguration['description']);
        self::assertArrayHasKey('rows', $targetConfiguration);
        self::assertEquals(2, count($targetConfiguration['rows']));
        self::assertEquals('foo', $targetConfiguration['rows'][0]['configuration']['queries'][0]);
        self::assertEquals('bar', $targetConfiguration['rows'][1]['configuration']['queries'][0]);
        self::assertArrayNotHasKey('_versions', $targetConfiguration);
        self::assertArrayNotHasKey('_versions', $targetConfiguration['rows'][0]);
    }

    public function testExecuteVersions(): void
    {
        $component = new Components($this->sapiClient);

        $config = new Configuration();
        $config->setComponentId('transformation');
        $config->setDescription('Test Configuration');
        $config->setConfigurationId('sapi-php-test');
        $config->setName('test-configuration');
        $configData = $component->addConfiguration($config);
        $config->setConfigurationId($configData['id']);

        $row = new ConfigurationRow($config);
        $row->setChangeDescription('Row 1');
        $row->setConfiguration(
            ['name' => 'test 1', 'backend' => 'docker', 'type' => 'r', 'queries' => ['foo']]
        );
        $component->addConfigurationRow($row);

        $row = new ConfigurationRow($config);
        $row->setChangeDescription('Row 2');
        $row->setConfiguration(
            ['name' => 'test 2', 'backend' => 'docker', 'type' => 'r', 'queries' => ['bar']]
        );
        $component->addConfigurationRow($row);

        $backup = new S3Backup($this->sapiClient, $this->s3Client);
        $backup->backupConfigs(TEST_AWS_S3_BUCKET, 'backup', true);

        $temp = new Temp();
        $temp->initRunFolder();

        $targetFile = $temp->createTmpFile('configurations.json');
        $this->s3Client->getObject([
            'Bucket' => TEST_AWS_S3_BUCKET,
            'Key' => 'backup/configurations.json',
            'SaveAs' => (string) $targetFile,
        ]);

        $targetContents = file_get_contents($targetFile);

        $targetData = json_decode($targetContents, true);
        $targetComponent = [];
        foreach ($targetData as $component) {
            if ($component['id'] == 'transformation') {
                $targetComponent = $component;
                break;
            }
        }
        self::assertGreaterThan(0, count($targetComponent));

        $targetConfiguration = [];
        foreach ($targetComponent['configurations'] as $configuration) {
            if ($configuration['name'] == 'test-configuration') {
                $targetConfiguration = $configuration;
            }
        }
        self::assertGreaterThan(0, count($targetConfiguration));
        self::assertEquals('Test Configuration', $targetConfiguration['description']);
        self::assertArrayNotHasKey('rows', $targetConfiguration);

        $configurationId = $targetConfiguration['id'];
        $targetFile = $temp->createTmpFile('configurations.json');
        $this->s3Client->getObject([
            'Bucket' => TEST_AWS_S3_BUCKET,
            'Key' => 'backup/configurations/transformation/' . $configurationId . '.json',
            'SaveAs' => (string) $targetFile,
        ]);
        $targetContents = file_get_contents($targetFile);
        $targetConfiguration = json_decode($targetContents, true);

        self::assertGreaterThan(0, count($targetConfiguration));
        self::assertEquals(3, $targetConfiguration['version']);
        self::assertEquals('test-configuration', $targetConfiguration['name']);
        self::assertEquals('Test Configuration', $targetConfiguration['description']);
        self::assertArrayHasKey('rows', $targetConfiguration);
        self::assertEquals(2, count($targetConfiguration['rows']));
        self::assertEquals('foo', $targetConfiguration['rows'][0]['configuration']['queries'][0]);
        self::assertEquals('bar', $targetConfiguration['rows'][1]['configuration']['queries'][0]);
        self::assertArrayHasKey('_versions', $targetConfiguration);
        self::assertEquals(3, count($targetConfiguration['_versions']));
        self::assertArrayHasKey('_versions', $targetConfiguration['rows'][0]);
        self::assertEquals(1, count($targetConfiguration['rows'][0]['_versions']));
    }

    /**
     * @dataProvider largeConfigurationsProvider
     * @param int $configurationRowsCount
     * @throws \Exception
     */
    public function testLargeConfigurations(int $configurationRowsCount): void
    {
        $component = new Components($this->sapiClient);

        $config = new Configuration();
        $config->setComponentId('transformation');
        $config->setDescription('Test Configuration');
        $config->setConfigurationId('sapi-php-test');
        $config->setName('test-configuration');
        $config->setState(['key' => 'value']);
        $configData = $component->addConfiguration($config);
        $config->setConfigurationId($configData['id']);

        $largeRowConfiguration = [
            'values' => [],
        ];
        $valuesCount = 100;
        for ($i = 0; $i < $valuesCount; $i++) {
            $largeRowConfiguration['values'][] = sha1(random_bytes(128));
        }

        for ($i = 0; $i < $configurationRowsCount; $i++) {
            $row = new ConfigurationRow($config);
            $row->setChangeDescription('Row 1');
            $row->setConfiguration($largeRowConfiguration);

            if ($i === 0) {
                $row->setState(['rowKey' => 'value']);
            }

            $component->addConfigurationRow($row);
        }

        $backup = new S3Backup($this->sapiClient, $this->s3Client);
        $backup->backupConfigs(TEST_AWS_S3_BUCKET, 'backup');

        $temp = new Temp();
        $temp->initRunFolder();

        $targetFile = $temp->createTmpFile($config->getConfigurationId() . 'configurations.json');
        $this->s3Client->getObject([
            'Bucket' => TEST_AWS_S3_BUCKET,
            'Key' => 'backup/configurations/transformation/' . $config->getConfigurationId() . '.json',
            'SaveAs' => (string) $targetFile,
        ]);
        $targetContents = file_get_contents($targetFile);
        $targetConfiguration = json_decode($targetContents, true);
        self::assertGreaterThan(0, count($targetConfiguration));
        self::assertEquals('test-configuration', $targetConfiguration['name']);
        self::assertEquals('Test Configuration', $targetConfiguration['description']);
        self::assertEquals(['key' => 'value'], $targetConfiguration['state']);
        self::assertArrayHasKey('rows', $targetConfiguration);
        self::assertCount($configurationRowsCount, $targetConfiguration['rows']);

        $firstRow = reset($targetConfiguration['rows']);
        $this->assertEquals(['rowKey' => 'value'], $firstRow['state']);
        $lastRow = end($targetConfiguration['rows']);
        $this->assertEmpty($lastRow['state']);
    }

    public function largeConfigurationsProvider(): array
    {
        return [
            [
                10,
            ],
            [
                30,
            ],
        ];
    }

    public function testPreserveEmptyObjectAndArray(): void
    {
        $component = new Components($this->sapiClient);

        $config = new Configuration();
        $config->setComponentId('transformation');
        $config->setDescription('Test Configuration');
        $config->setConfigurationId('sapi-php-test');
        $config->setName('test-configuration');
        $config->setConfiguration(
            [
                "dummyObject" => new \stdClass(),
                "dummyArray" => [],
            ]
        );
        $configData = $component->addConfiguration($config);
        $config->setConfigurationId($configData['id']);

        $row = new ConfigurationRow($config);
        $row->setChangeDescription('Row 1');
        $row->setConfiguration(
            [
                'name' => 'test 1',
                'backend' => 'docker',
                'type' => 'r',
                'queries' => ['foo'],
                "dummyObject" => new \stdClass(),
                "dummyArray" => [],
            ]
        );
        $component->addConfigurationRow($row);

        $row = new ConfigurationRow($config);
        $row->setChangeDescription('Row 2');
        $row->setConfiguration(
            [
                'name' => 'test 2',
                'backend' => 'docker',
                'type' => 'r',
                'queries' => ['bar'],
                "dummyObject" => new \stdClass(),
                "dummyArray" => [],
            ]
        );
        $component->addConfigurationRow($row);

        $backup = new S3Backup($this->sapiClient, $this->s3Client);
        $backup->backupConfigs(TEST_AWS_S3_BUCKET, 'backup', false);

        $temp = new Temp();
        $temp->initRunFolder();

        $targetFile = $temp->createTmpFile('configurations.json');
        $this->s3Client->getObject([
            'Bucket' => TEST_AWS_S3_BUCKET,
            'Key' => 'backup/configurations.json',
            'SaveAs' => (string) $targetFile,
        ]);
        $targetContents = file_get_contents($targetFile);
        $targetData = json_decode($targetContents);
        $targetConfiguration = $targetData[0]->configurations[0];

        self::assertEquals(new \stdClass(), $targetConfiguration->configuration->dummyObject);
        self::assertEquals([], $targetConfiguration->configuration->dummyArray);

        $configurationId = $targetConfiguration->id;
        $targetFile = $temp->createTmpFile($configurationId . 'configurations.json');
        $this->s3Client->getObject([
            'Bucket' => TEST_AWS_S3_BUCKET,
            'Key' => 'backup/configurations/transformation/' . $configurationId . '.json',
            'SaveAs' => (string) $targetFile,
        ]);
        $targetContents = file_get_contents($targetFile);
        $targetConfiguration = json_decode($targetContents);

        self::assertEquals(new \stdClass(), $targetConfiguration->rows[0]->configuration->dummyObject);
        self::assertEquals([], $targetConfiguration->rows[0]->configuration->dummyArray);
    }

    //@FIXME backup table skip tests

    public function testExecuteLinkedBuckets(): void
    {
        $bucketId = $this->sapiClient->createBucket("main", StorageApi::STAGE_IN);

        $this->sapiClient->setBucketAttribute($bucketId, 'key', 'value', true);
        $this->sapiClient->shareBucket($bucketId, ['sharing' => 'organization']);

        $this->sapiClient->createTable("in.c-main", "sample", new CsvFile(__DIR__ . "/data/sample.csv"));

        $token = $this->sapiClient->verifyToken();
        $projectId = $token['owner']['id'];

        $this->sapiClient->linkBucket('linked', 'in', $projectId, $bucketId);

        $backup = new S3Backup($this->sapiClient, $this->s3Client);
        $backup->backupTablesMetadata(TEST_AWS_S3_BUCKET, 'backup');

        $temp = new Temp();
        $temp->initRunFolder();

        $targetFile = $temp->createTmpFile('buckets.json');
        $this->s3Client->getObject([
            'Bucket' => TEST_AWS_S3_BUCKET,
            'Key' => 'backup/buckets.json',
            'SaveAs' => (string) $targetFile,
        ]);
        $buckets = json_decode(file_get_contents($targetFile), true);

        self::assertCount(2, $buckets);
        self::assertNotEmpty($buckets[1]['sourceBucket']);

        $targetFile = $temp->createTmpFile('tables.json');
        $this->s3Client->getObject([
            'Bucket' => TEST_AWS_S3_BUCKET,
            'Key' => 'backup/tables.json',
            'SaveAs' => (string) $targetFile,
        ]);

        $tables = json_decode(file_get_contents($targetFile), true);

        self::assertCount(2, $tables);
        self::assertNotEmpty($tables[1]['sourceTable']);
    }

    public function testExecuteMetadata(): void
    {
        $this->sapiClient->createBucket("main", StorageApi::STAGE_IN);
        $this->sapiClient->createTable("in.c-main", "sample", new CsvFile(__DIR__ . "/data/sample.csv"));

        $metadata = new Metadata($this->sapiClient);
        $metadata->postBucketMetadata("in.c-main", "system", [
            [
                "key" => "bucketKey",
                "value" => "bucketValue",
            ],
        ]);
        $metadata->postTableMetadata("in.c-main.sample", "system", [
            [
                "key" => "tableKey",
                "value" => "tableValue",
            ],
        ]);
        $metadata->postColumnMetadata("in.c-main.sample.col1", "system", [
            [
                "key" => "columnKey",
                "value" => "columnValue",
            ],
        ]);

        $backup = new S3Backup($this->sapiClient, $this->s3Client);
        $backup->backupTablesMetadata(TEST_AWS_S3_BUCKET, 'backup');

        $temp = new Temp();
        $temp->initRunFolder();

        $targetFile = $temp->createTmpFile('buckets.json');
        $this->s3Client->getObject([
            'Bucket' => TEST_AWS_S3_BUCKET,
            'Key' => 'backup/buckets.json',
            'SaveAs' => (string) $targetFile,
        ]);

        $data = json_decode(file_get_contents($targetFile), true);
        $this->assertEquals("bucketKey", $data[0]["metadata"][0]["key"]);
        $this->assertEquals("bucketValue", $data[0]["metadata"][0]["value"]);

        $targetFile = $temp->createTmpFile('tables.json');
        $this->s3Client->getObject([
            'Bucket' => TEST_AWS_S3_BUCKET,
            'Key' => 'backup/tables.json',
            'SaveAs' => (string) $targetFile,
        ]);
        $data = json_decode(file_get_contents($targetFile), true);
        $this->assertEquals("tableKey", $data[0]["metadata"][0]["key"]);
        $this->assertEquals("tableValue", $data[0]["metadata"][0]["value"]);
        $this->assertEquals("columnKey", $data[0]["columnMetadata"]["col1"][0]["key"]);
        $this->assertEquals("columnValue", $data[0]["columnMetadata"]["col1"][0]["value"]);
    }

    public function testExecuteWithoutPath(): void
    {
        $this->sapiClient->createBucket("main", StorageApi::STAGE_IN);
        $this->sapiClient->createTable("in.c-main", "sample", new CsvFile(__DIR__ . "/data/sample.csv"));

        $backup = new S3Backup($this->sapiClient, $this->s3Client);
        $backup->backupTablesMetadata(TEST_AWS_S3_BUCKET, null);
        $backup->backupConfigs(TEST_AWS_S3_BUCKET, null);

        $keys = array_map(function ($key) {
            return $key["Key"];
        }, $this->s3Client->listObjects([
            'Bucket' => TEST_AWS_S3_BUCKET,
        ])->toArray()["Contents"]);

        self::assertTrue(in_array('buckets.json', $keys));
        self::assertTrue(in_array('tables.json', $keys));
        self::assertTrue(in_array('configurations.json', $keys));
        self::assertCount(3, $keys);
    }

    private function cleanupKbcProject()
    {
        $components = new Components($this->sapiClient);
        foreach ($components->listComponents() as $component) {
            foreach ($component['configurations'] as $configuration) {
                $components->deleteConfiguration($component['id'], $configuration['id']);

                // delete configuration from trash
                $components->deleteConfiguration($component['id'], $configuration['id']);
            }
        }

        // drop linked buckets
        foreach ($this->sapiClient->listBuckets() as $bucket) {
            if (isset($bucket['sourceBucket'])) {
                $this->sapiClient->dropBucket($bucket["id"], ["force" => true]);
            }
        }

        foreach ($this->sapiClient->listBuckets() as $bucket) {
            $this->sapiClient->dropBucket($bucket["id"], ["force" => true]);
        }
    }

    private function cleanupS3()
    {
        $keys = $this->s3Client->listObjects(['Bucket' => TEST_AWS_S3_BUCKET])->toArray();
        if (isset($keys['Contents'])) {
            $keys = $keys['Contents'];
        } else {
            $keys = [];
        }

        $deleteObjects = [];
        foreach ($keys as $key) {
            $deleteObjects[] = $key;
        }

        if (count($deleteObjects) > 0) {
            $this->s3Client->deleteObjects(
                [
                    'Bucket' => TEST_AWS_S3_BUCKET,
                    'Delete' => ['Objects' => $deleteObjects],
                ]
            );
        }

    }
}
