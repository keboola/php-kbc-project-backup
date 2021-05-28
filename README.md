# Keboola Connection Project Backup PHP

PHP library for easy backup of KBC project into Amazon Simple Cloud Storage Service‎ (S3)

## Usage

Library is available as composer package.

### Installation

```bash
composer require keboola/php-storage-api-backup
```

## Development

Clone github repository and build Docker container 

```
git clone https://github.com/keboola/php-storage-api-backup.git
cd php-storage-api-backup
docker-compose build
```

Create `.env` file from this template

```bash
TEST_AWS_STORAGE_API_URL=
TEST_AWS_STORAGE_API_TOKEN=
TEST_AWS_ACCESS_KEY_ID=
TEST_AWS_SECRET_ACCESS_KEY=
TEST_AWS_REGION=
TEST_AWS_S3_BUCKET=
TEST_AZURE_STORAGE_API_URL=
TEST_AZURE_STORAGE_API_TOKEN=
TEST_AZURE_ACCOUNT_NAME=
TEST_AZURE_ACCOUNT_KEY=
TEST_AZURE_CONTAINER_NAME=
```

- `TEST_AWS_STORAGE_API_*` variables are from the project with AWS S3 backend whhich you want to backup
- `TEST_AWS_*` variables are from the S3 bucket the backup will be stored to _(Use [aws-cf-template.json](./aws-cf-template.json) CloudFormation stack template to create all required AWS resources)_
- `TEST_AZURE_STORAGE_API_*` variables are from the project with Azure Blob storage backend which you want to backup
- `TEST_AZURE_ACCOUNT_` create new Storage Account in your Azure Subscription
- `TEST_AZURE_CONTAINER_NAME` create new Container in the Storage account

```bash
docker-compose run --rm tests
```
