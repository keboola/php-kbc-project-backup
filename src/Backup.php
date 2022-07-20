<?php

declare(strict_types=1);

namespace Keboola\ProjectBackup;

use Exception;
use Keboola\ProjectBackup\Exception\SkipTableException;
use Keboola\ProjectBackup\FileClient\AbsFileClient;
use Keboola\ProjectBackup\FileClient\IFileClient;
use Keboola\ProjectBackup\FileClient\S3FileClient;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\HandlerStack;
use Keboola\StorageApi\Options\Components\ListConfigurationMetadataOptions;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

abstract class Backup
{
    private const CONFIGURATION_PAGING_LIMIT = 2;

    protected LoggerInterface $logger;

    protected Client $sapiClient;

    protected BranchAwareClient $branchAwareClient;

    private array $skipTables = [
        'in.c-from_L0_Sourcing.RAYNET_ORIGINAL_SEGMENT_MAPPING',
        'out.c-to_L2_DATAMARTS.PRINT_SOLAR_PRODEJE_PLAN',
        'out.c-To_L3_Distribuce.PRINT_SOLAR_PRODEJE_PLAN',
        'in.c-from_L0_Sourcing_Inzerce.cnc-keboola-mapping-tables-device',
        'in.c-from_L0_Source_Advertisement_data.MEDIAPLANNER',
        'in.c-from_L0_Sourcing_marketing.PRICE_LIST',
        'in.c-from_L0_sourcing.CISELNIK_TITLE_MAPPING',
        'in.c-from_L0_Sourcing_Inzerce.GAM_VYUZITELNOST_CELKEM',
        'out.c-TO_L3_Publicis.VOLNY_PRODEJ_PNS',
        'in.c-from_L0_Sourcing.ARES_FIRMY',
        'in.c-from_L0_Sourcing.DISTRIBUTION_BY_VENDOR',
        'in.c-from_L0_Sourcing.AD_INTEL_EXPORTS',
        'in.c-from_L0_Sourcing.ADINTEL_POTENTIAL_SEGMENTATION',
        'in.c-from_L0_sourcing.PRINT_TITLE_IMAGE_URL',
        'in.c-from_L0_Sourcing_Inzerce.caroda_instream',
        'in.c-from_L0_Sourcing.PREMIUM_BUDGET_DATA',
        'out.c-To_L3_Distribuce.SOLAR_CALENDAR',
        'in.c-from_L0_Sourcing_Inzerce.PRISM_VIDEO_DAY',
        'in.c-from_L0_Sourcing.PREMIUM_CELK_STATS_DATA',
        'in.c-from_L0_sourcing.CISELNIK_WEB_MAPPING',
        'in.c-from_L0_Sourcing_marketing.TITLE_SAP',
        'in.c-from_L0_Sourcing.cross-client-list',
        'in.c-from_L0_Source_Advertisement_data.MAPPING_GALLERY_POSITION',
        'in.c-from_L0_Sourcing.CARODA',
        'in.c-From_L1_Sales.SOLAR_SUBSCRIBERS_TRANSACTIONS',
        'in.c-from_L0_Sourcing.mediaprojekt_union',
        'in.c-from_L0_Sourcing.private-deals',
        'in.c-from_L0_Sourcing_Inzerce.CPEX_BUYERS_DAILY',
        'in.c-from_L0_sourcing.PRINT_SUBSCRIBERS_DARKY',
        'in.c-from_L0_Sourcing.GAM_LINEITEM',
        'in.c-test_premium.PREMIUM_KONVERZE',
        'in.c-from_L0_sourcing.PRINT_SOLAR_BUDGET',
        'in.c-from_L0_Sourcing_Inzerce.AD_RLS_CLIENTS_AGENCY',
        'in.c-from_L0_Confidential.SOLAR_SUBS_MARKETING_DATA',
        'in.c-from_L0_sourcing.daktela_tickets',
        'in.c-from_L1_Confidential.PNL',
        'in.c-from_L0_sourcing.CISELNIK_TITLE_GROUP',
        'in.c-from_L0_sourcing.PRINT_PRODEJE_KRIZOVKY',
        'in.c-from_L3_INZERCE.SAS_KAMPANE',
        'in.c-from_L3_INZERCE.04_master_cpex_alias',
        'in.c-from_L0_Sourcing.zone-line-items',
        'in.c-from_L0_Sourcing.MP_UNION_BLESK_VS_KONKURENCE',
        'in.c-from_L0_Sourcing.cross-event-list',
        'in.c-from_L0_Sourcing.pubmatic_hb-tagname',
        'in.c-from_L0_sourcing.daktela_activities',
        'in.c-from_L0_sourcing.CISELNIK_WEB_GROUPS',
        'in.c-from_L0_Sourcing.UNIFIED_SUBSCRIBERS',
        'in.c-from_L0_Source_Advertisement_data.PRINT_AD_DOHADY_TRANSFER',
        'in.c-from_L0_Source_Advertisement_data.ONLINE_AD_FIX_PROVISION',
        'in.c-from_L0_Sourcing_Inzerce.reach_order',
        'in.c-from_L0_Sourcing_Inzerce.rtb-od-jonase-sas-zdroj-rtb-jako-sas-zdroj',
        'in.c-from_L0_sourcing.PRINT_SUBSCRIBERS_REKLAMACE',
        'in.c-from_L0_Source_Advertisement_data.MANUAL_MAPPING_ADINTEL_TO_INSIDER',
        'out.c-to_L2_DATAMARTS.PRINT_SOLAR_PRODEJE',
        'in.c-from_L0_Sourcing.rtb-fakturace',
        'in.c-from_L0_Sourcing_Inzerce.reach_li_lifetime',
        'in.c-from_L0_Sourcing.GAM_AD_EXCHANGE',
        'in.c-from_L0_Sourcing_Inzerce.WEEK_NETMONITOR_ALIAS',
        'out.c-To_L3_Chytra_distribuce.SOLAR_CALENDAR',
        'in.c-from_L0_sourcing.plan-print-plan-ikiosek',
        'in.c-from_L0_Sourcing.BT_CROSS_REACH',
        'in.c-from_L0_Sourcing_marketing.MARKETING_ACTUAL',
        'in.c-from_L0_Sourcing_marketing.MEDIA_COST_SAP_2021',
        'in.c-from_L0_Sourcing.seznam-klientu-adintel-inzerce-cnc',
        'in.c-from_L0_Source_Advertisement_data.PRINT_AD_FIX_PROVISION',
        'in.c-From_L1_Sales.PNL_PRINT_POC',
        'in.c-from_L0_Sourcing.BT_TITLES_DETAIL',
        'in.c-from_L0_Sourcing_Inzerce.OMS_ORDER_LI',
        'in.c-from_L0_Sourcing.DATA_ABC_CR_AGGREGATED',
        'in.c-from_L0_Sourcing.CISELNIK_TITLE_MAPPING',
        'in.c-from_L0_sourcing.PRINT_SOLAR_OSTATNI_PRODEJNI_KANALY',
        'in.c-from_L0_Sourcing.GAM_Y_PUBMATIC_PRIVATE_DEALS',
        'in.c-from_L0_Sourcing_Inzerce.GAM_MASTER',
        'in.c-from_L0_Sourcing_Inzerce.GAM_YIELD_PARTNERS',
        'in.c-from_L0_Sourcing_Inzerce.PUBMATIC_API_HEADER_BIDDING_158123',
        'in.c-from_L0_Confidential.PNL',
        'in.c-from_L0_Sourcing_marketing.media-costs-2019-2021',
        'in.c-From_L1_Sales.PRINT_MONITORING_PNS',
        'in.c-from_L0_Sourcing_Inzerce.GAM_CODE_SERVED_COUNT',
        'in.c-from_L0_Source_Advertisement_data.PRINT_AD_PCT_PROVISION',
        'in.c-from_L0_sourcing.PRINT_ABC_CR_AGGREGATED',
        'in.c-from_L3_INZERCE.site',
        'in.c-from_L0_sourcing.PRINT_DISTRIBUTION_SALES',
        'in.c-from_L0_Sourcing.MEDIA_PROJEKT_ROCNI_PIVOT',
        'in.c-from_L0_Sourcing_marketing.MARKETING_BUDGET',
        'in.c-from_L1_Content.CNC_FB_SOCDEMO',
        'in.c-from_L0_Sourcing.PUBMATIC_ALL',
        'in.c-from_L0_Sourcing.RUIAN_KRAJE',
        'in.c-from_L0_sourcing.PRINT_SUBSCRIBERS_CESKAPOSTA',
        'in.c-from_L0_Sourcing_marketing.geko-2019-2021',
        'in.c-from_L0_Sourcing_Inzerce.RAYNET_ORGANIGRAM',
        'in.c-from_L0_Sourcing.order-id_productlineitems',
        'in.c-from_L0_Sourcing_marketing.MEDIA_COSTS',
        'in.c-from_L0_Sourcing.CNC_MAPPING_SEGMENTACE',
        'in.c-from_L0_sourcing.obce-psc-kraje_alias',
        'in.c-from_L0_Sourcing_marketing.CROSS_PROMO_LIST',
        'in.c-from_L1_Content.PLAN_MONTH',
        'in.c-from_L0_Sourcing_Inzerce.GAM_INSIDER',
        'in.c-from_L0_Sourcing.MEDIAPROJEKT_UNION_ROCNI',
        'in.c-from_L0_Sourcing.RAYNET_ECONOMY_ACTIVITY',
        'in.c-from_L0_Sourcing_Inzerce.DTF_DETAIL_URL_TOP20',
        'in.c-from_L0_Sourcing_Inzerce.CISELNIK_WEB_GROUPS_CLEANED',
        'in.c-from_L0_Sourcing.CISELNIK_TITLE_GROUPS',
        'in.c-from_L0_Sourcing.BT_SOCDEM_DETAIL',
        'in.c-From_L1_Finance.ABC_CR',
        'in.c-from_L0_Sourcing_Inzerce.CISELNIK_WEB_MAPPING_CLEANED',
        'in.c-from_L0_sourcing.PRINT_SOLAR_CALENDAR',
        'out.c-To_L3_Chytra_distribuce.POC_PNL',
        'in.c-from_L0_Sourcing_Inzerce.PRISM_VIDEO_MONTH',
        'in.c-from_L0_sourcing.PRINT_SOLAR_PRODEJE',
        'in.c-from_L0_Sourcing_marketing.MEDIA_CONTRACTS',
        'out.c-To_L3_Chytra_distribuce.ABC_CR_DATA',
        'in.c-from_L0_Sourcing_Inzerce.TRIPLELIFT_CPEX',
        'in.c-from_L0_Source_Advertisement_data.ADINTEL_TO_INSIDER_SUBJECT_MAPPING',
        'in.c-from_L0_Sourcing.caroda-outstream',
        'in.c-from_L0_Sourcing.pubmatic_publisheradtag',
        'in.c-from_L0_Sourcing.MML_TGI_UNION_PIVOT',
        'in.c-from_L0_Sourcing.SALES_TEAM',
        'in.c-from_L0_Sourcing.PUBMATIC_MONTHLY',
        'in.c-from_L0_Sourcing.INSIDER_EDITION_DETAIL',
        'in.c-from_L0_Sourcing.PREMIUM_REVENUES',
        'in.c-from_L0_sourcing.SOLAR_SUBSCRIBERS_ACTUALS_PLAN',
        'in.c-from_L0_Sourcing_Inzerce.GAM_MASTER_DIRECT',
        'in.c-from_L0_Sourcing_marketing.MARKETING_BUDGET_2021',
        'in.c-from_L0_Sourcing_Inzerce.bonusy-rezervy',
        'in.c-test_premium.PREMIUM_DATA_CHURN_DRIVERS',
        'in.c-from_L0_sourcing.CISELNIK_PSC',
        'in.c-from_L0_Sourcing.PUBMATIC_API_MONTHLY',
        'in.c-from_L0_Sourcing_marketing.TITLES_RESPONSIBLE',
        'in.c-from_L0_sourcing.PRINT_SUBSCRIBERS_DENNIDATA',
        'in.c-from_L0_Sourcing_Inzerce.AD_REVENUES_PLAN_BREAKDOWN',
        'in.c-from_L0_Sourcing_marketing.HDS',
        'in.c-from_L0_Sourcing.sap_bw_mapping',
        'in.c-from_L0_Sourcing_Inzerce.pubmatic_ad_tags',
        'in.c-from_L0_Sourcing_Inzerce.MONTH_NETMONITOR',
        'in.c-from_L0_Sourcing_Inzerce.reach_ad_unit',
        'out.c-DATASENTICS_POC.ABC_CR_DATA',
        'in.c-from_L0_Sourcing.RUIAN_ADRESNI_MISTA',
        'in.c-from_L0_sourcing.PRINT_SOLAR_PRODEJE_PLAN',
        'in.c-from_L0_Sourcing.PUBMATIC_DEALS_MONTHLY',
        'in.c-from_L0_Sourcing_Inzerce.CISELNIK_BISKO_SEGMENTS',
        'in.c-from_L3_INZERCE.05_master_adform_alias',
        'in.c-from_L0_Sourcing_Inzerce.DT_IMPRESSIONS',
        'in.c-from_L0_Sourcing_Inzerce.day_netmonitor',
        'in.c-from_L0_Sourcing_marketing.MARKETING_FORECAST',
        'in.c-from_L0_Sourcing_marketing.MEDIA_BRAND_NEW',
        'in.c-From_L1_Finance.VOLNY_PRODEJ_PNS',
        'in.c-from_L0_Sourcing.ADVERTISEMENT_SALES_BUDGET',
        'in.c-from_L0_Sourcing.CPEX_BUYERS_DAILY_4499',
        'in.c-from_L0_Sourcing.DATA_ABC_CR',
        'in.c-from_L0_Sourcing_marketing.MARKETING_ACTUAL_2021',
        'in.c-from_L0_Sourcing_Inzerce.reach_li_week',
        'in.c-from_L0_Sourcing.CMS_BUDGET_URL_aggregated',
        'in.c-from_L0_Sourcing_Inzerce.reach_order_week',
        'in.c-From_L0_sourcing.VOLNY_PRODEJ_PNS_ALLDATA',
        'out.c-TO_L3_Publicis.ABC_CR',
        'in.c-from_L1_Content.PLAN_DAY',
        'in.c-from_L0_Sourcing_Inzerce.rtb-fakturace-source',
        'in.c-from_L0_Sourcing.PUBMATIC_GAM_Y_DEAL_TYPE',
        'in.c-from_L3_INZERCE.ONLINE_INZERCE_REVENUES',
        'in.c-from_L1_Content.CONTENT_OVERVIEW',
        'in.c-from_L3_INZERCE.index_report',
        'in.c-from_L3_INZERCE.triplelift_alias',
        'in.c-from_L0_Sourcing_marketing.GEKO',
        'in.c-from_L0_Confidential.SOLAR_SUBS_COMPLAINTS',
        'in.c-from_L0_Sourcing.CMS_DATA',
        'in.c-From_L1_Sales.SOLAR_CALENDAR',
        'in.c-from_L0_Source_Advertisement_data.MEDIAPLANNER_KEYWORDS',
        'in.c-from_L0_Sourcing_Inzerce.klienti-vm-seznam-klientu',
        'in.c-from_L0_Sourcing.DISTRIBUTION_AGGREGATED',
        'in.c-from_L0_Sourcing_marketing.hds-2019-2021',
        'in.c-from_L0_Sourcing_Inzerce.cpex-dobropisy',
        'in.c-from_L0_Confidential.SOLAR_SUBS_TRANSACTIONS',
        'in.c-from_L0_Source_Advertisement_data.ONLINE_AD_PCT_PROVOSION',
        'out.c-To_L3_Distribuce.PRINT_MONITORING_PNS',
        'in.c-from_L0_Sourcing.PERFORMAX',
        'in.c-from_L0_Sourcing.ADMONITOR_MAPPING_SEGMENTACE',
        'in.c-from_L0_Sourcing.NETMONITOR_DATA',
        'in.c-from_L0_Sourcing_Inzerce.GAM_MASTER_RTB',
        'in.c-from_L3_INZERCE.06_master_pubmatic',
        'in.c-from_L0_Sourcing.ORDERS_INSIDER_NEW',
        'in.c-From_L1_Sales.PRINT_SOLAR_PRODEJE_PLAN',
        'in.c-from_L0_sourcing.PRINT_MONITORING_PNS',
        'out.c-to_L2_DATAMARTS.print_solar_budget_alias',
        'in.c-from_L0_Sourcing_Inzerce.rtb-od-jonase-sas-zdroj-budget-do-tbl',
        'in.c-from_L0_Sourcing.INSIDER_SUBJECT',
        'in.c-from_L0_Sourcing_Inzerce.RTB_FEE_ADJUSTMENTS',
        'out.c-dev-pubmatic-api.PUBMATIC_ALL_20210711151805',
        'out.c-prod-sap_daily.PNL_2020_partial',
        'out.c-sap_rfc_dev.FINANCE_ZRASMT_MARKETING',
        'out.c-prod-sap_daily.PNL_2022_partial',
        'out.c-inzerce_staging.09_MASTER_GAM_NON-RTB',
        'out.c-hledejceny.ITEMS_SK_DAILY',
        'out.c-prod-sap_daily.PNL_2021_partial',
    ];

    public function __construct(Client $sapiClient, ?LoggerInterface $logger)
    {
        $this->sapiClient = $sapiClient;
        $this->logger = $logger ?: new NullLogger();

        $devBranches = new DevBranches($this->sapiClient);
        $listBranches = $devBranches->listBranches();
        $defaultBranch = current(array_filter($listBranches, fn($v) => $v['isDefault'] === true));

        $this->branchAwareClient = new BranchAwareClient(
            $defaultBranch['id'],
            [
                'url' => $sapiClient->getApiUrl(),
                'token' => $sapiClient->getTokenString(),
            ]
        );
    }

    /**
     * @param string|resource $content
     */
    abstract protected function putToStorage(string $name, $content): void;

    public function backupTable(string $tableId): void
    {
        try {
            $fileInfo = $this->getTableFileInfo($tableId);
        } catch (SkipTableException $e) {
            return;
        }

        $fileClient = $this->getFileClient($fileInfo);
        if ($fileInfo['isSliced'] === true) {
            // Download manifest with all sliced files
            $client = new \GuzzleHttp\Client([
                'handler' => HandlerStack::create([
                    'backoffMaxTries' => 10,
                ]),
            ]);
            $manifest = json_decode($client->get($fileInfo['url'])->getBody()->getContents(), true);

            foreach ($manifest['entries'] as $i => $part) {
                $this->putToStorage(
                    sprintf(
                        '%s.part_%d.csv.gz',
                        str_replace('.', '/', $tableId),
                        $i
                    ),
                    $fileClient->getFileContent($part)
                );
            }
        } else {
            $this->putToStorage(
                str_replace('.', '/', $tableId) . '.csv.gz',
                $fileClient->getFileContent()
            );
        }
    }

    protected function getTableFileInfo(string $tableId): array
    {
        $table = $this->sapiClient->getTable($tableId);

        if ($table['bucket']['stage'] === 'sys') {
            $this->logger->warning(sprintf('Skipping table %s (sys bucket)', $table['id']));
            throw new SkipTableException();
        }

        if ($table['isAlias']) {
            $this->logger->warning(sprintf('Skipping table %s (alias)', $table['id']));
            throw new SkipTableException();
        }

        $this->logger->info(sprintf('Exporting table %s', $tableId));

        $fileId = $this->sapiClient->exportTableAsync($tableId, [
            'gzip' => true,
        ]);

        return (array) $this->sapiClient->getFile(
            $fileId['file']['id'],
            (new GetFileOptions())->setFederationToken(true)
        );
    }

    public function backupTablesMetadata(): void
    {
        $this->logger->info('Exporting buckets');

        $this->putToStorage(
            'buckets.json',
            (string) json_encode($this->sapiClient->listBuckets(['include' => 'attributes,metadata']))
        );

        $this->logger->info('Exporting tables');
        $tables = $this->sapiClient->listTables(null, [
            'include' => 'attributes,columns,buckets,metadata,columnMetadata',
        ]);

        $tables = array_filter($tables, fn($v) => !in_array($v['id'], $this->skipTables));

        $this->putToStorage('tables.json', (string) json_encode($tables));
    }

    public function backupConfigs(bool $includeVersions = true): void
    {
        $this->logger->info('Exporting configurations');

        $tmp = new Temp();
        $tmp->initRunFolder();

        $configurationsFile = $tmp->createFile('configurations.json');

        // use raw api call to prevent parsing json - preserve empty JSON objects
        $this->sapiClient->apiGet('components?include=configuration', $configurationsFile->getPathname());
        $handle = fopen((string) $configurationsFile, 'r');
        if ($handle) {
            $this->putToStorage('configurations.json', '{}');
            fclose($handle);
        } else {
            throw new Exception(sprintf('Cannot open file %s', (string) $configurationsFile));
        }
    }

    protected function getFileClient(array $fileInfo): IFileClient
    {
        if (isset($fileInfo['credentials'])) {
            return new S3FileClient($fileInfo);
        } elseif (isset($fileInfo['absCredentials'])) {
            return new AbsFileClient($fileInfo);
        } else {
            throw new Exception('Unknown file storage client.');
        }
    }
}
