# php-kbc-project-backup – AI Development Context

## What this repository does

Low-level PHP library for backing up a Keboola project to cloud storage. Used by `app-project-backup` as its backup engine.

## Documentation

- **`docs/overview.md`** – public methods, what is backed up, backup structure in storage, what is skipped
- **`docs/how-it-works.md`** – step-by-step backup flow (backupTable flow, FileClient selection, putToStorage per backend)

## Required environment variables

Before running tests, verify that these variables are present in `.env` (or exported in the shell). If missing, ask for them explicitly.

**For AWS S3 tests:**

| Variable | Description |
|---|---|
| `TEST_AWS_STORAGE_API_URL` | URL of the Keboola project on AWS stack |
| `TEST_AWS_STORAGE_API_TOKEN` | Storage token of the project on AWS stack |
| `TEST_AWS_ACCESS_KEY_ID` | AWS Access Key ID |
| `TEST_AWS_SECRET_ACCESS_KEY` | AWS Secret Access Key |
| `TEST_AWS_REGION` | AWS region (e.g. `us-east-1`) |
| `TEST_AWS_S3_BUCKET` | S3 bucket name for test backups |

**For Azure ABS tests:**

| Variable | Description |
|---|---|
| `TEST_AZURE_STORAGE_API_URL` | URL of the Keboola project on Azure stack |
| `TEST_AZURE_STORAGE_API_TOKEN` | Storage token of the project on Azure stack |
| `TEST_AZURE_ACCOUNT_NAME` | Azure storage account name |
| `TEST_AZURE_ACCOUNT_KEY` | Azure storage key |
| `TEST_AZURE_CONTAINER_NAME` | Azure Blob Storage container name |

**For GCS tests:**

| Variable | Description |
|---|---|
| `TEST_GCP_STORAGE_API_URL` | URL of the Keboola project on GCP stack |
| `TEST_GCP_STORAGE_API_TOKEN` | Storage token of the project on GCP stack |
| `TEST_GCP_BUCKET` | GCS bucket name for test backups |
| `TEST_GCP_SERVICE_ACCOUNT` | JSON service account key (full JSON as string) |

> Check that the `.env` file exists in the repo root. If not, create it based on the list above.

## Development commands

Service name in `docker-compose.yml` is `tests`. There is no `composer tests` script – tests are run per-backend.

```bash
docker compose run --rm tests composer phpcs
docker compose run --rm tests composer phpstan
docker compose run --rm tests composer test-aws             # S3 integration tests
docker compose run --rm tests composer test-azure           # ABS integration tests
docker compose run --rm tests composer test-gcp             # GCS integration tests
docker compose run --rm tests composer static-analysis      # validate + phplint + phpcs + phpstan
```

## Key files

| File | Purpose |
|---|---|
| `src/Backup.php` | Abstract class – all backup logic |
| `src/S3Backup.php` | AWS S3 implementation |
| `src/AbsBackup.php` | Azure Blob Storage implementation |
| `src/GcsBackup.php` | Google Cloud Storage implementation |
| `src/FileClient/` | Clients for downloading table files |
| `src/NotificationClient.php` | Keboola Notification API communication |

## Architecture

Abstract class `Backup` + concrete implementations for each backend (S3Backup, AbsBackup, GcsBackup). Separate file clients for accessing storage files.

## Coding standards

- PHP 8.x with strict types
- PHPStan level max
- Keboola coding standard (PSR-12)

## Related repositories

- Used in: `app-project-backup`
- Paired with: `php-kbc-project-restore`
