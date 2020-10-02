<?php

declare(strict_types=1);

error_reporting(E_ALL);
set_error_handler(function ($errno, $errstr, $errfile, $errline, array $errcontext): bool {
    if (!(error_reporting() & $errno)) {
        // respect error_reporting() level
        // libraries used in custom components may emit notices that cannot be fixed
        return false;
    }
    throw new ErrorException($errstr, 0, $errno, $errfile, $errline);
});

$environments = [
    'TEST_STORAGE_API_URL',
    'TEST_STORAGE_API_TOKEN',
    'TEST_AWS_ACCESS_KEY_ID',
    'TEST_AWS_SECRET_ACCESS_KEY',
    'TEST_AWS_REGION',
    'TEST_AWS_S3_BUCKET',
];

foreach ($environments as $environment) {
    if (empty(getenv($environment))) {
        throw new ErrorException(sprintf('Missing environment "%s".', $environment));
    }
}

require __DIR__ . '/../vendor/autoload.php';
