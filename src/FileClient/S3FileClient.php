<?php

declare(strict_types=1);

namespace Keboola\ProjectBackup\FileClient;

use Aws\S3\S3Client;
use Exception;
use Keboola\Temp\Temp;

class S3FileClient implements IFileClient
{
    private S3Client $s3Client;

    private string $bucket;

    private string $fileKey;

    private string $tmpFilePath;

    private Temp $tmp;

    public function __construct(array $fileInfo)
    {
        $this->bucket = $fileInfo['s3Path']['bucket'];
        $this->fileKey = $fileInfo['s3Path']['key'];
        $this->s3Client = new S3Client([
            'version' => 'latest',
            'region' => $fileInfo['region'],
            'credentials' => [
                'key' => $fileInfo['credentials']['AccessKeyId'],
                'secret' => $fileInfo['credentials']['SecretAccessKey'],
                'token' => $fileInfo['credentials']['SessionToken'],
            ],
        ]);
        $this->tmp = new Temp();
        $this->tmpFilePath = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . uniqid('sapi-export-');
    }

    /** @return resource */
    public function getFileContent(?array $filePart = null)
    {
        if ($filePart) {
            $fileKey = substr($filePart['url'], strpos($filePart['url'], '/', 5) + 1);
            $filePath = sprintf(
                '%s_%s',
                $this->tmpFilePath,
                md5(str_replace('/', '_', $fileKey)),
            );
        } else {
            $fileKey = $this->fileKey;
            $filePath = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . uniqid('table');
        }

        $this->s3Client->getObject([
            'Bucket' => $this->bucket,
            'Key' => $fileKey,
            'SaveAs' => $filePath,
        ]);

        $file = fopen($filePath, 'r');
        if (!$file) {
            throw new Exception(sprintf('Cannot open file %s', $filePath));
        }
        return $file;
    }
}
