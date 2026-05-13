# php-kbc-project-backup – how the backup works

## Initializing the Backup class

```php
new S3Backup($sapiClient, $logger)  // or AbsBackup / GcsBackup
```

`Backup::__construct()` constructor:
1. Stores `sapiClient`
2. Fetches branch list (`DevBranches::listBranches()`)
3. Finds the default branch (`isDefault === true`)
4. Creates `BranchAwareClient` for operations on the default branch

## Backup flow

```
backupProjectMetadata()
  └─ DevBranchesMetadata::listBranchMetadata()
  └─ putToStorage('defaultBranchMetadata.json', json)

backupTablesMetadata()
  ├─ listBuckets() → putToStorage('buckets.json', json)
  └─ listTables()  → putToStorage('tables.json', json)

backupTable($tableId)
  └─ see section below

backupConfigs($includeVersions)
  └─ see section below

backupTriggers()
  └─ listTriggers() → putToStorage('triggers.json', json)

backupNotifications()
  └─ filters only default branch notifications
  └─ putToStorage('notifications.json', json)

backupPermanentFiles()
  └─ listFiles(isPermanent: true) → forEach: putToStorage('files/<fileId>', stream)
  └─ putToStorage('permanentFiles.json', json)

backupSignedUrls()   [GcsBackup only]
  └─ putToStorage('signedUrls.json', json)
```

## backupTable – detail

```
getTableFileInfo($tableId):
  1. getTable($tableId)            – table metadata
  2. Skip checks:
     - stage === 'sys'             → SkipTableException (warning, continues)
     - hasExternalSchema           → SkipTableException
     - sourceBucket (DataCatalog)  → SkipTableException
     - isAlias                     → SkipTableException
  3. exportTableAsync($tableId, gzip: true)  – SAPI async export
  4. getFile($fileId, federationToken: true) → fileInfo with cloud credentials

getFileClient($fileInfo):
  - fileInfo.credentials present       → S3FileClient
  - fileInfo.absCredentials present    → AbsFileClient
  - fileInfo.gcsCredentials present    → GcsFileClient

if isSliced:
  GET manifest URL → foreach entry → putToStorage(table.part_N.csv.gz, chunk)
else:
  putToStorage(stage/c-bucket/table.csv.gz, fileClient.getFileContent())
```

### Why federationToken

`getFile(federationToken: true)` returns temporary cloud credentials (AWS STS token, ABS SAS token, or GCS access token). FileClient then accesses cloud storage directly without going through SAPI. Without federationToken, every byte would have to pass through the SAPI HTTP API, which is significantly slower for large tables.

### Paths in storage

```
stage/c-bucketName/tableName.csv.gz          (non-sliced)
stage/c-bucketName/tableName.part_0.csv.gz   (sliced, part 0)
stage/c-bucketName/tableName.part_1.csv.gz   (sliced, part 1)
...
```

Dots in `tableId` (e.g. `in.c-bucket.table`) are replaced with slashes:
```php
str_replace('.', '/', $tableId)  →  in/c-bucket/table
```

## backupConfigs – detail

```
Load all components with configurations (apiGet 'components?include=configuration')

foreach component:
  foreach configuration:
    putToStorage('configurations/<componentId>/<configId>.json', data + rows + state)

    if includeVersions:
      paginated (CONFIGURATION_PAGING_LIMIT = 2 per page):
        putToStorage(version data)

    putToStorage('configurations/<componentId>/<configId>.json.metadata', metadata)
```

`CONFIGURATION_PAGING_LIMIT = 2` – intentionally low limit, but applies **only to configuration versions**, not to loading the configurations themselves (those are always fetched in a single call).

## putToStorage – implementation per backend

Abstract method, each backend implements it differently:

| Backend | Method | Parameters |
|---------|--------|-----------|
| `S3Backup` | `S3Client::putObject()` | Bucket, Key (= path), Body (= content) |
| `AbsBackup` | `BlobRestProxy::createBlockBlob()` | container, blobName, content |
| `GcsBackup` | `Bucket->object($name)->upload($content)` | GCS bucket object upload |

Content (`$content`) is either `string` (JSON files) or `resource` (binary file streams).

## FileClient – client selection

```
IFileClient
  ├─ S3FileClient   – getFileContent() via S3Client::getObject()
  ├─ AbsFileClient  – getFileContent() via BlobRestProxy::getBlob()
  └─ GcsFileClient  – getFileContent() via StorageClient
```

Client selection depends on fields in `fileInfo` returned by SAPI:
- `credentials` field → S3FileClient (AWS)
- `absCredentials` field → AbsFileClient (Azure)
- `gcsCredentials` field → GcsFileClient (Google Cloud)

## What is skipped and why

| Condition | Reason for skipping |
|----------|-----------------|
| `stage === 'sys'` | Sys buckets are system-level, do not belong to the user |
| `isAlias === true` | Alias has no own data, references another bucket |
| `hasExternalSchema` | Data is managed externally (Snowflake external table) |
| `sourceBucket` not null | Data Catalog table – data is in the source bucket |
| Notifications outside default branch | Branch notifications are not migrated |
