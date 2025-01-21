<?php

declare(strict_types=1);

error_reporting(E_ALL);
set_error_handler(function ($errno, $errstr, $errfile, $errline): bool {
    if (!(error_reporting() & $errno)) {
        // respect error_reporting() level
        // libraries used in custom components may emit notices that cannot be fixed
        return false;
    }
    throw new ErrorException($errstr, 0, $errno, $errfile, $errline);
});

$environments = [
    'TEST_AWS_STORAGE_API_URL',
    'TEST_AWS_STORAGE_API_TOKEN',
    'TEST_AZURE_STORAGE_API_URL',
    'TEST_AZURE_STORAGE_API_TOKEN',
    'TEST_AWS_ACCESS_KEY_ID',
    'TEST_AWS_SECRET_ACCESS_KEY',
    'TEST_AWS_REGION',
    'TEST_AWS_S3_BUCKET',
    'TEST_AZURE_ACCOUNT_NAME',
    'TEST_AZURE_ACCOUNT_KEY',
    'TEST_GCP_STORAGE_API_URL',
    'TEST_GCP_STORAGE_API_TOKEN',
    'TEST_GCP_BUCKET',
    'TEST_GCP_SERVICE_ACCOUNT',
];

foreach ($environments as $environment) {
    if (empty(getenv($environment))) {
        throw new ErrorException(sprintf('Missing environment "%s".', $environment));
    }
}

require __DIR__ . '/../vendor/autoload.php';
