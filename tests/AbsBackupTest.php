<?php

declare(strict_types=1);

namespace Keboola\ProjectBackup\Tests;

use Keboola\Csv\CsvFile;
use Keboola\ProjectBackup\AbsBackup;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\Temp\Temp;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\Blob;
use MicrosoftAzure\Storage\Blob\Models\Container;
use MicrosoftAzure\Storage\Common\Middlewares\RetryMiddlewareFactory;
use PHPUnit\Framework\TestCase;
use stdClass;

class AbsBackupTest extends TestCase
{
    private Client $sapiClient;

    private BlobRestProxy $absClient;

    private const TEST_CONTAINER_NAME = 'project-migration-test-container';

    public function setUp(): void
    {
        parent::setUp();

        $this->sapiClient = new Client([
            'url' => getenv('TEST_AZURE_STORAGE_API_URL'),
            'token' => getenv('TEST_AZURE_STORAGE_API_TOKEN'),
        ]);

        $this->cleanupKbcProject();

        $this->absClient = BlobRestProxy::createBlobService(sprintf(
            'DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net',
            (string) getenv('TEST_AZURE_ACCOUNT_NAME'),
            (string) getenv('TEST_AZURE_ACCOUNT_KEY')
        ));
        $this->absClient->pushMiddleware(
            RetryMiddlewareFactory::create()
        );

        $containers = $this->absClient->listContainers();
        $listContainers = array_map(fn(Container $v) => $v->getName(), $containers->getContainers());

        if (!in_array(self::TEST_CONTAINER_NAME, $listContainers)) {
            $this->absClient->createContainer(self::TEST_CONTAINER_NAME);
        }
        $this->cleanupAbs();
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

        $backup = new AbsBackup(
            $this->sapiClient,
            $this->absClient,
            self::TEST_CONTAINER_NAME
        );
        $backup->backupConfigs(false);

        $temp = new Temp();
        $temp->initRunFolder();

        $targetContents = $this->absClient->getBlob(self::TEST_CONTAINER_NAME, 'configurations.json');

        $targetData = json_decode(
            (string) stream_get_contents($targetContents->getContentStream()),
            true
        );
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
        $targetContents = $this->absClient->getBlob(
            self::TEST_CONTAINER_NAME,
            'configurations/transformation/' . $configurationId . '.json'
        );

        $targetConfiguration = json_decode(
            (string) stream_get_contents($targetContents->getContentStream()),
            true
        );

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

        $backup = new AbsBackup(
            $this->sapiClient,
            $this->absClient,
            self::TEST_CONTAINER_NAME
        );
        $backup->backupConfigs(true);

        $temp = new Temp();
        $temp->initRunFolder();

        $targetContents = $this->absClient->getBlob(self::TEST_CONTAINER_NAME, 'configurations.json');

        $targetData = json_decode(
            (string) stream_get_contents($targetContents->getContentStream()),
            true
        );
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
        $targetContents = $this->absClient->getBlob(
            self::TEST_CONTAINER_NAME,
            'configurations/transformation/' . $configurationId . '.json'
        );
        $targetConfiguration = json_decode(
            (string) stream_get_contents($targetContents->getContentStream()),
            true
        );

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

        $backup = new AbsBackup(
            $this->sapiClient,
            $this->absClient,
            self::TEST_CONTAINER_NAME
        );
        $backup->backupConfigs();

        $temp = new Temp();
        $temp->initRunFolder();

        $targetContents = $this->absClient->getBlob(
            self::TEST_CONTAINER_NAME,
            'configurations/transformation/' . $config->getConfigurationId() . '.json'
        );
        $targetConfiguration = json_decode(
            (string) stream_get_contents($targetContents->getContentStream()),
            true
        );
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

        $backup = new AbsBackup(
            $this->sapiClient,
            $this->absClient,
            self::TEST_CONTAINER_NAME
        );
        $backup->backupConfigs(false);

        $temp = new Temp();
        $temp->initRunFolder();

        $targetContents = $this->absClient->getBlob(self::TEST_CONTAINER_NAME, 'configurations.json');
        $targetData = json_decode((string) stream_get_contents($targetContents->getContentStream()));
        $targetConfiguration = $targetData[0]->configurations[0];

        self::assertEquals(new stdClass(), $targetConfiguration->configuration->dummyObject);
        self::assertEquals([], $targetConfiguration->configuration->dummyArray);

        $configurationId = $targetConfiguration->id;
        $targetContents = $this->absClient->getBlob(
            self::TEST_CONTAINER_NAME,
            'configurations/transformation/' . $configurationId . '.json'
        );
        $targetConfiguration = json_decode((string) stream_get_contents($targetContents->getContentStream()));

        self::assertEquals(new stdClass(), $targetConfiguration->rows[0]->configuration->dummyObject);
        self::assertEquals([], $targetConfiguration->rows[0]->configuration->dummyArray);
    }

    public function testExecuteLinkedBuckets(): void
    {
        $bucketId = $this->sapiClient->createBucket('main', Client::STAGE_IN);

        $this->sapiClient->setBucketAttribute($bucketId, 'key', 'value', true);
        $this->sapiClient->shareBucket($bucketId, ['sharing' => 'organization']);

        $this->sapiClient->createTable('in.c-main', 'sample', new CsvFile(__DIR__ . '/data/sample.csv'));

        $token = $this->sapiClient->verifyToken();
        $projectId = $token['owner']['id'];

        $this->sapiClient->linkBucket('linked', 'in', $projectId, $bucketId);

        $backup = new AbsBackup(
            $this->sapiClient,
            $this->absClient,
            self::TEST_CONTAINER_NAME
        );
        $backup->backupTablesMetadata();

        $temp = new Temp();
        $temp->initRunFolder();

        $targetContents = $this->absClient->getBlob(self::TEST_CONTAINER_NAME, 'buckets.json');

        $buckets = json_decode(
            (string) stream_get_contents($targetContents->getContentStream()),
            true
        );

        self::assertCount(2, $buckets);
        self::assertNotEmpty($buckets[1]['sourceBucket']);

        $targetContents = $this->absClient->getBlob(self::TEST_CONTAINER_NAME, 'tables.json');

        $tables = json_decode(
            (string) stream_get_contents($targetContents->getContentStream()),
            true
        );

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

        $backup = new AbsBackup(
            $this->sapiClient,
            $this->absClient,
            self::TEST_CONTAINER_NAME
        );
        $backup->backupTablesMetadata();

        $temp = new Temp();
        $temp->initRunFolder();

        $targetContents = $this->absClient->getBlob(self::TEST_CONTAINER_NAME, 'buckets.json');
        $data = json_decode(
            (string) stream_get_contents($targetContents->getContentStream()),
            true
        );
        $this->assertEquals('bucketKey', $data[0]['metadata'][0]['key']);
        $this->assertEquals('bucketValue', $data[0]['metadata'][0]['value']);

        $targetContents = $this->absClient->getBlob(self::TEST_CONTAINER_NAME, 'tables.json');
        $data = json_decode(
            (string) stream_get_contents($targetContents->getContentStream()),
            true
        );
        $this->assertEquals('tableKey', $data[0]['metadata'][0]['key']);
        $this->assertEquals('tableValue', $data[0]['metadata'][0]['value']);
        $this->assertEquals('columnKey', $data[0]['columnMetadata']['col1'][0]['key']);
        $this->assertEquals('columnValue', $data[0]['columnMetadata']['col1'][0]['value']);
    }

    public function testExecuteWithoutPath(): void
    {
        $this->sapiClient->createBucket('main', Client::STAGE_IN);
        $this->sapiClient->createTable('in.c-main', 'sample', new CsvFile(__DIR__ . '/data/sample.csv'));

        $backup = new AbsBackup(
            $this->sapiClient,
            $this->absClient,
            self::TEST_CONTAINER_NAME
        );
        $backup->backupTablesMetadata();
        $backup->backupConfigs();

        $blobs = $this->absClient->listBlobs(self::TEST_CONTAINER_NAME);
        $listBlobs = array_map(fn(Blob $v) => $v->getName(), $blobs->getBlobs());

        self::assertTrue(in_array('buckets.json', $listBlobs));
        self::assertTrue(in_array('tables.json', $listBlobs));
        self::assertTrue(in_array('configurations.json', $listBlobs));
        self::assertCount(3, $listBlobs);
    }

    private function cleanupKbcProject(): void
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
                $this->sapiClient->dropBucket($bucket['id'], ['force' => true]);
            }
        }

        foreach ($this->sapiClient->listBuckets() as $bucket) {
            $this->sapiClient->dropBucket($bucket['id'], ['force' => true]);
        }
    }

    private function cleanupAbs(): void
    {
        $blobs = $this->absClient->listBlobs(self::TEST_CONTAINER_NAME);

        foreach ($blobs->getBlobs() as $blob) {
            $this->absClient->deleteBlob(self::TEST_CONTAINER_NAME, $blob->getName());
        }
    }
}
