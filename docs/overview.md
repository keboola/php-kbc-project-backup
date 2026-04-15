# php-kbc-project-backup – overview

## What the library does

Low-level PHP library for backing up a Keboola project to cloud storage. Contains no Keboola-specific boilerplate – it is pure backup logic that can be used from any PHP code.

Used by `app-project-backup` as its backup engine.

## Architecture

Abstract class `Backup` contains all logic. Concrete storage backends extend it:

```
Backup (abstract)
  ├─ S3Backup      – AWS S3
  ├─ AbsBackup     – Azure Blob Storage
  └─ GcsBackup     – Google Cloud Storage
```

Separate file clients exist for direct access to table files in storage (via federated credentials):

```
FileClient (interface)
  ├─ S3FileClient
  ├─ AbsFileClient
  └─ GcsFileClient
```

## Public methods

| Method | What it saves to storage |
|---|---|
| `backupProjectMetadata()` | `defaultBranchMetadata.json` |
| `backupTablesMetadata()` | `buckets.json`, `tables.json` |
| `backupTable(string $tableId)` | gzip CSV data for one table |
| `backupConfigs(bool $includeVersions)` | `configurations/<componentId>/<configId>.json` |
| `backupTriggers()` | `triggers.json` |
| `backupNotifications()` | `notifications.json` |
| `backupPermanentFiles()` | `files/<fileId>` + `permanentFiles.json` |
| `backupSignedUrls()` | `signedUrls.json` (GCS without user credentials only) |

## What is skipped

- Sys bucket tables (`stage === 'sys'`)
- Alias tables (`isAlias === true`)
- External schema tables (`hasExternalSchema === true`)
- Data Catalog tables (bucket has `sourceBucket`)
- Notifications not belonging to the default branch

## Backup structure in storage

```
<backup_path>/
  ├─ defaultBranchMetadata.json
  ├─ buckets.json
  ├─ tables.json
  ├─ configurations.json         (index of all components with configurations)
  ├─ triggers.json
  ├─ notifications.json
  ├─ permanentFiles.json
  ├─ signedUrls.json             (GCS backend only)
  ├─ configurations/
  │    └─ <componentId>/
  │         ├─ <configId>.json
  │         └─ <configId>.json.metadata
  ├─ in/
  │    └─ c-<bucketName>/
  │         ├─ <tableName>.csv.gz          (non-sliced)
  │         └─ <tableName>.part_0.csv.gz   (sliced)
  └─ files/
       └─ <fileId>
```

## Key files

| File | Description |
|---|---|
| `src/Backup.php` | Abstract class – all backup logic |
| `src/S3Backup.php` | AWS S3 implementation |
| `src/AbsBackup.php` | Azure Blob Storage implementation |
| `src/GcsBackup.php` | Google Cloud Storage implementation |
| `src/FileClient/` | Clients for direct table file downloads from storage |
| `src/NotificationClient.php` | Keboola Notification API communication |

## Development and testing

Service name in `docker-compose.yml` is `tests`. There is no `composer tests` script – tests are split per-backend.

```bash
docker compose run --rm tests composer phpcs
docker compose run --rm tests composer phpstan
docker compose run --rm tests composer test-aws           # S3 tests
docker compose run --rm tests composer test-azure         # ABS tests
docker compose run --rm tests composer test-gcp           # GCS tests
docker compose run --rm tests composer static-analysis    # validate + phplint + phpcs + phpstan
```

Integration tests require live Keboola credentials (a project on each of the three cloud stacks + credentials for the respective storage).

## Related repositories

- Used in: `app-project-backup`
- Paired with: `php-kbc-project-restore`
