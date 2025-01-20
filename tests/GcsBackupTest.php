<?php

declare(strict_types=1);

namespace Keboola\ProjectBackup\Tests;

use Google\Cloud\Storage\StorageClient;
use Google\Cloud\Storage\StorageObject;
use Keboola\Csv\CsvFile;
use Keboola\NotificationClient\Requests\PostSubscription\EmailRecipient;
use Keboola\NotificationClient\Requests\PostSubscription\Filter;
use Keboola\NotificationClient\Requests\Subscription;
use Keboola\ProjectBackup\GcsBackup;
use Keboola\ProjectBackup\NotificationClient;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\DevBranchesMetadata;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationMetadata;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Tokens;
use Keboola\Temp\Temp;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;

class GcsBackupTest extends TestCase
{
    use CleanupKbcProject;

    protected Client $sapiClient;

    protected BranchAwareClient $branchAwareClient;

    private StorageClient $storageClient;

    public function setUp(): void
    {
        parent::setUp();

        $this->sapiClient = new Client([
            'url' => getenv('TEST_GCP_STORAGE_API_URL'),
            'token' => getenv('TEST_GCP_STORAGE_API_TOKEN'),
        ]);

        $devBranches = new DevBranches($this->sapiClient);
        $listBranches = $devBranches->listBranches();
        $defaultBranch = current(array_filter($listBranches, fn($v) => $v['isDefault'] === true));

        $this->branchAwareClient = new BranchAwareClient(
            $defaultBranch['id'],
            [
                'url' => getenv('TEST_GCP_STORAGE_API_URL'),
                'token' => getenv('TEST_GCP_STORAGE_API_TOKEN'),
            ],
        );

        $this->cleanupKbcProject();

        $this->storageClient = new StorageClient([
            'keyFile' => json_decode((string) getenv('TEST_GCP_SERVICE_ACCOUNT'), true),
        ]);

        $this->cleanupGCS();
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
            ['name' => 'test 1', 'backend' => 'docker', 'type' => 'r', 'queries' => ['foo']],
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

        $backup = new GcsBackup(
            $this->sapiClient,
            $this->storageClient,
            (string) getenv('TEST_GCP_BUCKET'),
            'backup/',
            true,
        );
        $backup->backupConfigs(false);

        $targetContents = $this->storageClient
            ->bucket((string) getenv('TEST_GCP_BUCKET'))
            ->object('backup/configurations.json')
            ->downloadAsString();

        /** @var array $targetData */
        $targetData = json_decode((string) $targetContents, true);

        $targetComponent = [];
        foreach ($targetData as $component) {
            if ($component['id'] === 'keboola.snowflake-transformation') {
                $targetComponent = $component;
                break;
            }
        }
        /** @var array $targetComponent */
        self::assertGreaterThan(0, count($targetComponent));

        $targetConfiguration = [];
        foreach ($targetComponent['configurations'] as $configuration) {
            if ($configuration['name'] === 'test-configuration') {
                $targetConfiguration = $configuration;
            }
        }
        /** @var array $targetConfiguration */
        self::assertGreaterThan(0, count($targetConfiguration));
        self::assertEquals('Test Configuration', $targetConfiguration['description']);
        self::assertArrayNotHasKey('rows', $targetConfiguration);

        $configurationId = $targetConfiguration['id'];

        $targetContents = $this->storageClient
            ->bucket((string) getenv('TEST_GCP_BUCKET'))
            ->object('backup/configurations/keboola.snowflake-transformation/' . $configurationId . '.json.metadata')
            ->downloadAsString();

        /** @var array $targetConfiguration */
        $targetConfiguration = json_decode((string) $targetContents, true);

        self::assertCount(1, $targetConfiguration);
        self::assertEquals('KBC.configuration.folderName', $targetConfiguration[0]['key']);
        self::assertEquals('testFolder', $targetConfiguration[0]['value']);
    }

    public function testExecuteNoVersions(): void
    {
        $component = new Components($this->sapiClient);

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

        $row = new ConfigurationRow($config);
        $row->setChangeDescription('Row 2');
        $row->setConfiguration(
            ['name' => 'test 2', 'backend' => 'docker', 'type' => 'r', 'queries' => ['bar']],
        );
        $component->addConfigurationRow($row);

        $backup = new GcsBackup(
            $this->sapiClient,
            $this->storageClient,
            (string) getenv('TEST_GCP_BUCKET'),
            'backup/',
            false,
        );
        $backup->backupConfigs(false);

        $targetContents = $this->storageClient
            ->bucket((string) getenv('TEST_GCP_BUCKET'))
            ->object('backup/configurations.json')
            ->downloadAsString();

        /** @var array $targetData */
        $targetData = json_decode((string) $targetContents, true);
        $targetComponent = [];
        foreach ($targetData as $component) {
            if ($component['id'] === 'keboola.snowflake-transformation') {
                $targetComponent = $component;
                break;
            }
        }
        /** @var array $targetComponent */
        self::assertGreaterThan(0, count($targetComponent));

        $targetConfiguration = [];
        foreach ($targetComponent['configurations'] as $configuration) {
            if ($configuration['name'] === 'test-configuration') {
                $targetConfiguration = $configuration;
            }
        }
        /** @var array $targetConfiguration */
        self::assertGreaterThan(0, count($targetConfiguration));
        self::assertEquals('Test Configuration', $targetConfiguration['description']);
        self::assertArrayNotHasKey('rows', $targetConfiguration);

        $configurationId = $targetConfiguration['id'];
        $targetContents = $this->storageClient
            ->bucket((string) getenv('TEST_GCP_BUCKET'))
            ->object('backup/configurations/keboola.snowflake-transformation/' . $configurationId . '.json')
            ->downloadAsString();

        /** @var array $targetConfiguration */
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

        $row = new ConfigurationRow($config);
        $row->setChangeDescription('Row 2');
        $row->setConfiguration(
            ['name' => 'test 2', 'backend' => 'docker', 'type' => 'r', 'queries' => ['bar']],
        );
        $component->addConfigurationRow($row);

        $backup = new GcsBackup(
            $this->sapiClient,
            $this->storageClient,
            (string) getenv('TEST_GCP_BUCKET'),
            'backup/',
            true,
        );
        $backup->backupConfigs(true);

        $targetContents = $this->storageClient
            ->bucket((string) getenv('TEST_GCP_BUCKET'))
            ->object('backup/configurations.json')
            ->downloadAsString();

        /** @var array $targetData */
        $targetData = json_decode((string) $targetContents, true);
        $targetComponent = [];
        foreach ($targetData as $component) {
            if ($component['id'] === 'keboola.snowflake-transformation') {
                $targetComponent = $component;
                break;
            }
        }
        /** @var array $targetComponent */
        self::assertGreaterThan(0, count($targetComponent));

        $targetConfiguration = [];
        foreach ($targetComponent['configurations'] as $configuration) {
            if ($configuration['name'] === 'test-configuration') {
                $targetConfiguration = $configuration;
            }
        }
        /** @var array $targetConfiguration */
        self::assertGreaterThan(0, count($targetConfiguration));
        self::assertEquals('Test Configuration', $targetConfiguration['description']);
        self::assertArrayNotHasKey('rows', $targetConfiguration);

        $configurationId = $targetConfiguration['id'];
        $targetContents = $this->storageClient
            ->bucket((string) getenv('TEST_GCP_BUCKET'))
            ->object('backup/configurations/keboola.snowflake-transformation/' . $configurationId . '.json')
            ->downloadAsString();

        /** @var array $targetConfiguration */
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

    #[DataProvider('largeConfigurationsProvider')]
    public function testLargeConfigurations(int $configurationRowsCount): void
    {
        $component = new Components($this->sapiClient);

        $config = new Configuration();
        $config->setComponentId('keboola.snowflake-transformation');
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

        $backup = new GcsBackup(
            $this->sapiClient,
            $this->storageClient,
            (string) getenv('TEST_GCP_BUCKET'),
            'backup/',
            true,
        );
        $backup->backupConfigs();

        $objectName = 'backup/configurations/keboola.snowflake-transformation/' .
            $config->getConfigurationId() . '.json';
        $targetContents = $this->storageClient
            ->bucket((string) getenv('TEST_GCP_BUCKET'))
            ->object($objectName)
            ->downloadAsString();

        /** @var array $targetConfiguration */
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

    public static function largeConfigurationsProvider(): array
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
        $config->setComponentId('keboola.snowflake-transformation');
        $config->setDescription('Test Configuration');
        $config->setConfigurationId('sapi-php-test');
        $config->setName('test-configuration');
        $config->setConfiguration(
            [
                'dummyObject' => new stdClass(),
                'dummyArray' => [],
            ],
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
            ],
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
            ],
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

        $targetContents = $this->storageClient
            ->bucket((string) getenv('TEST_GCP_BUCKET'))
            ->object('backup/configurations.json')
            ->downloadAsString();
        /** @var array $targetData */
        $targetData = json_decode((string) $targetContents);
        $targetConfiguration = $targetData[0]->configurations[0];

        self::assertEquals(new stdClass(), $targetConfiguration->configuration->dummyObject);
        self::assertEquals([], $targetConfiguration->configuration->dummyArray);

        $configurationId = $targetConfiguration->id;
        $targetContents = $this->storageClient
            ->bucket((string) getenv('TEST_GCP_BUCKET'))
            ->object('backup/configurations/keboola.snowflake-transformation/' . $configurationId . '.json')
            ->downloadAsString();

        /** @var stdClass $targetConfiguration */
        $targetConfiguration = json_decode((string) $targetContents);

        self::assertEquals(new stdClass(), $targetConfiguration->rows[0]->configuration->dummyObject);
        self::assertEquals([], $targetConfiguration->rows[0]->configuration->dummyArray);
    }

    //@FIXME backup table skip tests

    public function testExecuteLinkedBuckets(): void
    {
        $bucketId = $this->sapiClient->createBucket('main', Client::STAGE_IN);

        $this->sapiClient->shareBucket($bucketId, ['sharing' => 'organization']);

        $this->sapiClient->createTable('in.c-main', 'sample', new CsvFile(__DIR__ . '/data/sample.csv'));

        $token = $this->sapiClient->verifyToken();
        $projectId = $token['owner']['id'];

        $this->sapiClient->linkBucket('linked', 'in', $projectId, $bucketId);

        $backup = new GcsBackup(
            $this->sapiClient,
            $this->storageClient,
            (string) getenv('TEST_GCP_BUCKET'),
            'backup/',
            true,
        );
        $backup->backupTablesMetadata();

        $targetFile = $this->storageClient
            ->bucket((string) getenv('TEST_GCP_BUCKET'))
            ->object('backup/buckets.json')
            ->downloadAsString();

        /** @var array $buckets */
        $buckets = json_decode($targetFile, true);

        self::assertCount(1, $buckets);
        self::assertArrayNotHasKey('sourceBucket', current($buckets));

        $targetFile = $this->storageClient
            ->bucket((string) getenv('TEST_GCP_BUCKET'))
            ->object('backup/tables.json')
            ->downloadAsString();

        /** @var array $tables */
        $tables = json_decode($targetFile, true);

        self::assertCount(1, $tables);
        self::assertArrayNotHasKey('sourceTable', current($tables));
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

        $backup = new GcsBackup(
            $this->sapiClient,
            $this->storageClient,
            (string) getenv('TEST_GCP_BUCKET'),
            'backup/',
            true,
        );
        $backup->backupTablesMetadata();

        $targetFile = $this->storageClient
            ->bucket((string) getenv('TEST_GCP_BUCKET'))
            ->object('backup/buckets.json')
            ->downloadAsString();

        /** @var array $data */
        $data = json_decode($targetFile, true);
        $this->assertEquals('bucketKey', $data[0]['metadata'][0]['key']);
        $this->assertEquals('bucketValue', $data[0]['metadata'][0]['value']);

        $targetFile = $this->storageClient
            ->bucket((string) getenv('TEST_GCP_BUCKET'))
            ->object('backup/tables.json')
            ->downloadAsString();

        /** @var array $data */
        $data = json_decode($targetFile, true);
        $this->assertEquals('tableKey', $data[0]['metadata'][0]['key']);
        $this->assertEquals('tableValue', $data[0]['metadata'][0]['value']);
        $this->assertEquals('columnKey', $data[0]['columnMetadata']['col1'][0]['key']);
        $this->assertEquals('columnValue', $data[0]['columnMetadata']['col1'][0]['value']);
    }

    public function testExecuteWithoutPath(): void
    {
        $this->sapiClient->createBucket('main', Client::STAGE_IN);
        $this->sapiClient->createTable('in.c-main', 'sample', new CsvFile(__DIR__ . '/data/sample.csv'));

        $backup = new GcsBackup(
            $this->sapiClient,
            $this->storageClient,
            (string) getenv('TEST_GCP_BUCKET'),
            'backup/',
            true,
        );
        $backup->backupTablesMetadata();
        $backup->backupConfigs();

        $objects = array_map(
            fn(StorageObject $object) => $object->name(),
            iterator_to_array($this->storageClient->bucket((string) getenv('TEST_GCP_BUCKET'))->objects()),
        );
        self::assertTrue(in_array('backup/buckets.json', $objects));
        self::assertTrue(in_array('backup/tables.json', $objects));
        self::assertTrue(in_array('backup/configurations.json', $objects));
        self::assertCount(3, $objects);
    }

    public function testBackupDefaultBranchMetadata(): void
    {
        $devBranchMetadata = [
            [
                'key' => 'KBC.projectDescription',
                'value' => 'project description',
            ],
        ];

        $branchMetadata = new DevBranchesMetadata($this->branchAwareClient);
        foreach ($branchMetadata->listBranchMetadata() as $item) {
            $branchMetadata->deleteBranchMetadata((int) $item['id']);
        }
        $branchMetadata->addBranchMetadata($devBranchMetadata);

        $backup = new GcsBackup(
            $this->sapiClient,
            $this->storageClient,
            (string) getenv('TEST_GCP_BUCKET'),
            'backup/',
            true,
        );
        $backup->backupProjectMetadata();

        $targetFile = $this->storageClient
            ->bucket((string) getenv('TEST_GCP_BUCKET'))
            ->object('backup/defaultBranchMetadata.json')
            ->downloadAsString();

        /** @var array $data */
        $data = json_decode($targetFile, true);

        self::assertEquals('KBC.projectDescription', $data[0]['key']);
        self::assertEquals('project description', $data[0]['value']);
    }

    public function testBackupPermanentFiles(): void
    {
        $fileContent = 'test-permanent-file';
        foreach ($this->sapiClient->listFiles() as $listFile) {
            $this->sapiClient->deleteFile($listFile['id']);
        }
        $tmp = new Temp();
        $file = $tmp->createFile('test.txt');
        file_put_contents($file->getPathname(), $fileContent);

        $fileOption = new FileUploadOptions();
        $fileOption->setIsPermanent(true);
        $fileOption->setTags(['tag1', 'tag2']);

        $this->sapiClient->uploadFile($file->getPathname(), $fileOption);
        sleep(3);
        $backup = new GcsBackup(
            $this->sapiClient,
            $this->storageClient,
            (string) getenv('TEST_GCP_BUCKET'),
            'backup/',
            true,
        );
        $backup->backupPermanentFiles();

        $data = $this->storageClient
            ->bucket((string) getenv('TEST_GCP_BUCKET'))
            ->object('backup/permanentFiles.json')
            ->downloadAsString();

        self::assertEquals('[{"name":"test.txt","tags":["tag1","tag2"]}]', $data);

        $data = $this->storageClient
            ->bucket((string) getenv('TEST_GCP_BUCKET'))
            ->object('backup/files/test.txt')
            ->downloadAsString();

        self::assertEquals($fileContent, $data);
    }

    public function testBackupTriggers(): void
    {
        $bucketId = $this->sapiClient->createBucket('main', Client::STAGE_IN);
        $tableId = $this->sapiClient->createTableAsync(
            $bucketId,
            'simple',
            new CsvFile(__DIR__ . '/data/sample.csv'),
        );

        $component = new Components($this->branchAwareClient);

        $configuration = new Configuration();
        $configuration->setComponentId('keboola.orchestrator');
        $configuration->setName('test-triggers');
        $orchestration = $component->addConfiguration($configuration);

        $token = new Tokens($this->sapiClient);
        $tokenOptions = new TokenCreateOptions();
        $tokenOptions->setDescription(sprintf('[_internal] Token for triggering %s', $orchestration['id']));
        $tokenOptions->setCanManageBuckets(true);
        $tokenOptions->setCanReadAllFileUploads(true);
        $tokenData = $token->createToken($tokenOptions);

        $this->sapiClient->createTrigger([
            'component' => 'keboola.orchestrator',
            'configurationId' => $orchestration['id'],
            'runWithTokenId' => $tokenData['id'],
            'tableIds' => [
                $tableId,
            ],
            'coolDownPeriodMinutes' => 5,
        ]);

        $backup = new GcsBackup(
            $this->sapiClient,
            $this->storageClient,
            (string) getenv('TEST_GCP_BUCKET'),
            'backup/',
            true,
        );
        $backup->backupTriggers();

        $targetFile = $this->storageClient
            ->bucket((string) getenv('TEST_GCP_BUCKET'))
            ->object('backup/triggers.json')
            ->downloadAsString();
        /** @var array{
         *     id: string,
         *     lastRun: string,
         *     creatorToken: string,
         *     runWithTokenId: int,
         *     component: string,
         *     configurationId: string,
         *     coolDownPeriodMinutes: int,
         *     tables: array{
         *          tableId: string,
         *     }[],
         * } $data
         */
        $data = (array) json_decode($targetFile, true);
        array_walk($data, function (&$item): void {
            unset($item['id'], $item['lastRun'], $item['creatorToken']);
        });

        $expectedConfig = [
            [
                'runWithTokenId' => $tokenData['id'],
                'component' => 'keboola.orchestrator',
                'configurationId' => $orchestration['id'],
                'coolDownPeriodMinutes' => 5,
                'tables' => [
                    [
                        'tableId' => $tableId,
                    ],
                ],
            ],
        ];

        self::assertEquals($expectedConfig, $data);
    }

    public function testBackupNotifications(): void
    {
        $notificationClient = new NotificationClient(
            $this->sapiClient->getServiceUrl('notification'),
            $this->sapiClient->getTokenString(),
            [
                'backoffMaxTries' => 3,
                'userAgent' => 'Keboola Project Backup',
            ],
        );

        $subcriptionRequest = new Subscription(
            'job-succeeded',
            new EmailRecipient('oj@oj.cz'),
            [
                new Filter('job.component.id', 'keboola.orchestrator'),
            ],
        );
        $notificationClient->createSubscription($subcriptionRequest);

        $devBranches = new DevBranches($this->sapiClient);
        $devBranch = $devBranches->createBranch('dev');
        $subcriptionRequest = new Subscription(
            'job-failed',
            new EmailRecipient('oj@oj.cz'),
            [
                new Filter('job.component.id', 'keboola.orchestrator'),
                new Filter('branch.id', (string) $devBranch['id']),
            ],
        );
        $notificationClient->createSubscription($subcriptionRequest);

        $backup = new GcsBackup(
            $this->sapiClient,
            $this->storageClient,
            (string) getenv('TEST_GCP_BUCKET'),
            'backup/',
            true,
        );
        $backup->backupNotifications();

        $targetFile = $this->storageClient
            ->bucket((string) getenv('TEST_GCP_BUCKET'))
            ->object('backup/notifications.json')
            ->downloadAsString();

        $data = (string) json_encode(
            (array) json_decode($targetFile, true),
            JSON_PRETTY_PRINT,
        );

        $expectedData = <<<JSON
[
    {
        "id": "%s",
        "event": "job-succeeded",
        "filters": [
            {
                "field": "branch.id",
                "value": "%s",
                "operator": "=="
            },
            {
                "field": "job.component.id",
                "value": "keboola.orchestrator",
                "operator": "=="
            }
        ],
        "recipient": {
            "channel": "email",
            "address": "oj@oj.cz"
        }
    }
]
JSON;

        self::assertStringMatchesFormat($expectedData, $data);
    }

    private function cleanupGCS(): void
    {
        $objects = $this->storageClient->bucket((string) getenv('TEST_GCP_BUCKET'))->objects();

        /** @var StorageObject $object */
        foreach ($objects as $object) {
            $object->delete();
        }
    }
}
