<?php

declare(strict_types=1);

namespace Keboola\ProjectBackup\FileClient;

interface IFileClient
{
    /** @return resource */
    public function getFileContent(?array $filePart = null);
}
