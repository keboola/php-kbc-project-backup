<?php

declare(strict_types=1);

namespace Keboola\ProjectBackup\Tests;

use Aws\S3\S3Client;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationMetadata;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\ProjectBackup\S3Backup;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use stdClass;

class S3BackupTest extends TestCase
{
    use CleanupKbcProject;

    protected Client $sapiClient;

    protected Client $branchAwareClient;

    private S3Client $s3Client;

    public function setUp(): void
    {
        parent::setUp();

        $this->sapiClient = new Client([
            'url' => getenv('TEST_AWS_STORAGE_API_URL'),
            'token' => getenv('TEST_AWS_STORAGE_API_TOKEN'),
        ]);

        $devBranches = new DevBranches($this->sapiClient);
        $listBranches = $devBranches->listBranches();
        $defaultBranch = current(array_filter($listBranches, fn($v) => $v['isDefault'] === true));

        $this->branchAwareClient = new BranchAwareClient(
            $defaultBranch['id'],
            [
                'url' => getenv('TEST_AZURE_STORAGE_API_URL'),
                'token' => getenv('TEST_AZURE_STORAGE_API_TOKEN'),
            ]
        );

        $this->cleanupKbcProject();

        putenv('AWS_ACCESS_KEY_ID=' . getenv('TEST_AWS_ACCESS_KEY_ID'));
        putenv('AWS_SECRET_ACCESS_KEY=' . getenv('TEST_AWS_SECRET_ACCESS_KEY'));

        $this->s3Client = new S3Client([
            'version' => 'latest',
            'region' => getenv('TEST_AWS_REGION'),
        ]);

        $this->cleanupS3();
    }


    public function testConfigurationMetadata(): void
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
            ['name' => 'test 1', 'backend' => 'docker', 'type' => 'r', 'queries' => ['foo']]
        );
        $component->addConfigurationRow($row);

        $configMetadata = new ConfigurationMetadata($config);
        $configMetadata->setMetadata([
            [
                'key' => 'KBC.configuration.folderName',
                'value' => 'testFolder',
            ],
        ]);

        $component->addConfigurationMetadata($configMetadata);

        $backup = new S3Backup(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'backup'
        );
        $backup->backupConfigs(false);

        $temp = new Temp();
        $temp->initRunFolder();

        $targetFile = $temp->createTmpFile('configurations.json');
        $this->s3Client->getObject([
            'Bucket' => getenv('TEST_AWS_S3_BUCKET'),
            'Key' => 'backup/configurations.json',
            'SaveAs' => (string) $targetFile,
        ]);

        $targetContents = file_get_contents((string) $targetFile);

        $targetData = json_decode((string) $targetContents, true);

        $targetComponent = [];
        foreach ($targetData as $component) {
            if ($component['id'] === 'keboola.snowflake-transformation') {
                $targetComponent = $component;
                break;
            }
        }
        self::assertGreaterThan(0, count($targetComponent));

        $targetConfiguration = [];
        foreach ($targetComponent['configurations'] as $configuration) {
            if ($configuration['name'] === 'test-configuration') {
                $targetConfiguration = $configuration;
            }
        }
        self::assertGreaterThan(0, count($targetConfiguration));
        self::assertEquals('Test Configuration', $targetConfiguration['description']);
        self::assertArrayNotHasKey('rows', $targetConfiguration);

        $configurationId = $targetConfiguration['id'];
        $targetFile = $temp->createTmpFile('configurations.json');
        $this->s3Client->getObject([
            'Bucket' => getenv('TEST_AWS_S3_BUCKET'),
            'Key' => 'backup/configurations/transformation/' . $configurationId . '.json.metadata',
            'SaveAs' => (string) $targetFile,
        ]);
        $targetContents = file_get_contents((string) $targetFile);
        $targetConfiguration = json_decode((string) $targetContents, true);

        self::assertCount(1, $targetConfiguration);
        self::assertEquals('KBC.configuration.folderName', $targetConfiguration[0]['key']);
        self::assertEquals('testFolder', $targetConfiguration[0]['value']);
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

        $backup = new S3Backup(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'backup'
        );
        $backup->backupConfigs(false);

        $temp = new Temp();
        $temp->initRunFolder();

        $targetFile = $temp->createTmpFile('configurations.json');
        $this->s3Client->getObject([
            'Bucket' => getenv('TEST_AWS_S3_BUCKET'),
            'Key' => 'backup/configurations.json',
            'SaveAs' => (string) $targetFile,
        ]);

        $targetContents = file_get_contents((string) $targetFile);

        $targetData = json_decode((string) $targetContents, true);
        $targetComponent = [];
        foreach ($targetData as $component) {
            if ($component['id'] === 'transformation') {
                $targetComponent = $component;
                break;
            }
        }
        self::assertGreaterThan(0, count($targetComponent));

        $targetConfiguration = [];
        foreach ($targetComponent['configurations'] as $configuration) {
            if ($configuration['name'] === 'test-configuration') {
                $targetConfiguration = $configuration;
            }
        }
        self::assertGreaterThan(0, count($targetConfiguration));
        self::assertEquals('Test Configuration', $targetConfiguration['description']);
        self::assertArrayNotHasKey('rows', $targetConfiguration);

        $configurationId = $targetConfiguration['id'];
        $targetFile = $temp->createTmpFile('configurations.json');
        $this->s3Client->getObject([
            'Bucket' => getenv('TEST_AWS_S3_BUCKET'),
            'Key' => 'backup/configurations/transformation/' . $configurationId . '.json',
            'SaveAs' => (string) $targetFile,
        ]);
        $targetContents = file_get_contents((string) $targetFile);
        $targetConfiguration = json_decode((string) $targetContents, true);

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

        $backup = new S3Backup(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'backup'
        );
        $backup->backupConfigs(true);

        $temp = new Temp();
        $temp->initRunFolder();

        $targetFile = $temp->createTmpFile('configurations.json');
        $this->s3Client->getObject([
            'Bucket' => getenv('TEST_AWS_S3_BUCKET'),
            'Key' => 'backup/configurations.json',
            'SaveAs' => (string) $targetFile,
        ]);

        $targetContents = file_get_contents((string) $targetFile);

        $targetData = json_decode((string) $targetContents, true);
        $targetComponent = [];
        foreach ($targetData as $component) {
            if ($component['id'] === 'transformation') {
                $targetComponent = $component;
                break;
            }
        }
        self::assertGreaterThan(0, count($targetComponent));

        $targetConfiguration = [];
        foreach ($targetComponent['configurations'] as $configuration) {
            if ($configuration['name'] === 'test-configuration') {
                $targetConfiguration = $configuration;
            }
        }
        self::assertGreaterThan(0, count($targetConfiguration));
        self::assertEquals('Test Configuration', $targetConfiguration['description']);
        self::assertArrayNotHasKey('rows', $targetConfiguration);

        $configurationId = $targetConfiguration['id'];
        $targetFile = $temp->createTmpFile('configurations.json');
        $this->s3Client->getObject([
            'Bucket' => getenv('TEST_AWS_S3_BUCKET'),
            'Key' => 'backup/configurations/transformation/' . $configurationId . '.json',
            'SaveAs' => (string) $targetFile,
        ]);
        $targetContents = file_get_contents((string) $targetFile);
        $targetConfiguration = json_decode((string) $targetContents, true);

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

        $backup = new S3Backup(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'backup'
        );
        $backup->backupConfigs();

        $temp = new Temp();
        $temp->initRunFolder();

        $targetFile = $temp->createTmpFile($config->getConfigurationId() . 'configurations.json');
        $this->s3Client->getObject([
            'Bucket' => getenv('TEST_AWS_S3_BUCKET'),
            'Key' => 'backup/configurations/transformation/' . $config->getConfigurationId() . '.json',
            'SaveAs' => (string) $targetFile,
        ]);
        $targetContents = file_get_contents((string) $targetFile);
        $targetConfiguration = json_decode((string) $targetContents, true);
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
                'dummyObject' => new stdClass(),
                'dummyArray' => [],
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
                'dummyObject' => new stdClass(),
                'dummyArray' => [],
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
                'dummyObject' => new stdClass(),
                'dummyArray' => [],
            ]
        );
        $component->addConfigurationRow($row);

        $backup = new S3Backup(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'backup'
        );
        $backup->backupConfigs(false);

        $temp = new Temp();
        $temp->initRunFolder();

        $targetFile = $temp->createTmpFile('configurations.json');
        $this->s3Client->getObject([
            'Bucket' => getenv('TEST_AWS_S3_BUCKET'),
            'Key' => 'backup/configurations.json',
            'SaveAs' => (string) $targetFile,
        ]);
        $targetContents = file_get_contents((string) $targetFile);
        $targetData = json_decode((string) $targetContents);
        $targetConfiguration = $targetData[0]->configurations[0];

        self::assertEquals(new stdClass(), $targetConfiguration->configuration->dummyObject);
        self::assertEquals([], $targetConfiguration->configuration->dummyArray);

        $configurationId = $targetConfiguration->id;
        $targetFile = $temp->createTmpFile($configurationId . 'configurations.json');
        $this->s3Client->getObject([
            'Bucket' => getenv('TEST_AWS_S3_BUCKET'),
            'Key' => 'backup/configurations/transformation/' . $configurationId . '.json',
            'SaveAs' => (string) $targetFile,
        ]);
        $targetContents = file_get_contents((string) $targetFile);
        $targetConfiguration = json_decode((string) $targetContents);

        self::assertEquals(new stdClass(), $targetConfiguration->rows[0]->configuration->dummyObject);
        self::assertEquals([], $targetConfiguration->rows[0]->configuration->dummyArray);
    }

    //@FIXME backup table skip tests

    public function testExecuteLinkedBuckets(): void
    {
        $bucketId = $this->sapiClient->createBucket('main', Client::STAGE_IN);

        $this->sapiClient->setBucketAttribute($bucketId, 'key', 'value', true);
        $this->sapiClient->shareBucket($bucketId, ['sharing' => 'organization']);

        $this->sapiClient->createTable('in.c-main', 'sample', new CsvFile(__DIR__ . '/data/sample.csv'));

        $token = $this->sapiClient->verifyToken();
        $projectId = $token['owner']['id'];

        $this->sapiClient->linkBucket('linked', 'in', $projectId, $bucketId);

        $backup = new S3Backup(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'backup'
        );
        $backup->backupTablesMetadata();

        $temp = new Temp();
        $temp->initRunFolder();

        $targetFile = $temp->createTmpFile('buckets.json');
        $this->s3Client->getObject([
            'Bucket' => getenv('TEST_AWS_S3_BUCKET'),
            'Key' => 'backup/buckets.json',
            'SaveAs' => (string) $targetFile,
        ]);
        $buckets = json_decode((string) file_get_contents((string) $targetFile), true);

        self::assertCount(2, $buckets);
        self::assertNotEmpty($buckets[1]['sourceBucket']);

        $targetFile = $temp->createTmpFile('tables.json');
        $this->s3Client->getObject([
            'Bucket' => getenv('TEST_AWS_S3_BUCKET'),
            'Key' => 'backup/tables.json',
            'SaveAs' => (string) $targetFile,
        ]);

        $tables = json_decode((string) file_get_contents((string) $targetFile), true);

        self::assertCount(2, $tables);
        self::assertNotEmpty($tables[1]['sourceTable']);
    }

    public function testExecuteMetadata(): void
    {
        $this->sapiClient->createBucket('main', Client::STAGE_IN);
        $this->sapiClient->createTable('in.c-main', 'sample', new CsvFile(__DIR__ . '/data/sample.csv'));

        $metadata = new Metadata($this->sapiClient);
        $metadata->postBucketMetadata('in.c-main', 'system', [
            [
                'key' => 'bucketKey',
                'value' => 'bucketValue',
            ],
        ]);
        $metadata->postTableMetadata('in.c-main.sample', 'system', [
            [
                'key' => 'tableKey',
                'value' => 'tableValue',
            ],
        ]);
        $metadata->postColumnMetadata('in.c-main.sample.col1', 'system', [
            [
                'key' => 'columnKey',
                'value' => 'columnValue',
            ],
        ]);

        $backup = new S3Backup(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'backup'
        );
        $backup->backupTablesMetadata();

        $temp = new Temp();
        $temp->initRunFolder();

        $targetFile = $temp->createTmpFile('buckets.json');
        $this->s3Client->getObject([
            'Bucket' => getenv('TEST_AWS_S3_BUCKET'),
            'Key' => 'backup/buckets.json',
            'SaveAs' => (string) $targetFile,
        ]);

        $data = json_decode((string) file_get_contents((string) $targetFile), true);
        $this->assertEquals('bucketKey', $data[0]['metadata'][0]['key']);
        $this->assertEquals('bucketValue', $data[0]['metadata'][0]['value']);

        $targetFile = $temp->createTmpFile('tables.json');
        $this->s3Client->getObject([
            'Bucket' => getenv('TEST_AWS_S3_BUCKET'),
            'Key' => 'backup/tables.json',
            'SaveAs' => (string) $targetFile,
        ]);
        $data = json_decode((string) file_get_contents((string) $targetFile), true);
        $this->assertEquals('tableKey', $data[0]['metadata'][0]['key']);
        $this->assertEquals('tableValue', $data[0]['metadata'][0]['value']);
        $this->assertEquals('columnKey', $data[0]['columnMetadata']['col1'][0]['key']);
        $this->assertEquals('columnValue', $data[0]['columnMetadata']['col1'][0]['value']);
    }

    public function testExecuteWithoutPath(): void
    {
        $this->sapiClient->createBucket('main', Client::STAGE_IN);
        $this->sapiClient->createTable('in.c-main', 'sample', new CsvFile(__DIR__ . '/data/sample.csv'));

        $backup = new S3Backup(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'backup'
        );
        $backup->backupTablesMetadata();
        $backup->backupConfigs();

        $keys = array_map(function ($key) {
            return $key['Key'];
        }, $this->s3Client->listObjects([
            'Bucket' => getenv('TEST_AWS_S3_BUCKET'),
        ])->toArray()['Contents']);

        self::assertTrue(in_array('backup/buckets.json', $keys));
        self::assertTrue(in_array('backup/tables.json', $keys));
        self::assertTrue(in_array('backup/configurations.json', $keys));
        self::assertCount(3, $keys);
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

        if (count($deleteObjects) > 0) {
            $this->s3Client->deleteObjects(
                [
                    'Bucket' => getenv('TEST_AWS_S3_BUCKET'),
                    'Delete' => ['Objects' => $deleteObjects],
                ]
            );
        }
    }
}
