<?php

declare(strict_types=1);

namespace Keboola\ProjectBackup\FileClient;

use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use SplFileInfo;

class AbsFileClient implements IFileClient
{
    private BlobRestProxy $absClient;

    private string $container;

    private string $name;

    public function __construct(array $fileInfo)
    {
        $this->absClient = BlobRestProxy::createBlobService($fileInfo['absCredentials']['SASConnectionString']);
        $this->container = $fileInfo['absPath']['container'];
        $this->name = $fileInfo['absPath']['name'];
    }

    /** @return resource */
    public function getFileContent(?array $filePart = null)
    {
        if ($filePart) {
            $partFileInfo = new SplFileInfo($filePart['url']);
            $filePath = $partFileInfo->getFilename();
        } else {
            $filePath = $this->name;
        }

        return $this->absClient
            ->getBlob(
                $this->container,
                $filePath
            )
            ->getContentStream()
        ;
    }
}
