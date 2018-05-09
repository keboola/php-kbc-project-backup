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

require __DIR__ . '/../vendor/autoload.php';
