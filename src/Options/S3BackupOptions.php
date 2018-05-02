<?php
namespace Keboola\ProjectBackup\Options;

class S3BackupOptions
{
    private $targetBucket;

    private $targetBasePath = null;

    private $onlyStructure = false;

    private $configVersionsLimit = 2;

    public function __construct($targetBucket)
    {
        $this->targetBucket = (string) $targetBucket;
    }

    public function getTargetBucket()
    {
        return $this->targetBucket;
    }

    public function setTargetBasePath($path = null)
    {
        $this->targetBasePath = $path;
        return $this;
    }

    public function getTargetBasePath()
    {
        return $this->targetBasePath;
    }

    public function setExportOnlyStructure(bool $value = true)
    {
        $this->onlyStructure = $value;
        return $this;
    }

    public function getExportOnlyStructure()
    {
        return $this->onlyStructure;
    }

    public function setExportConfigVersionsLimit(int $value = 2)
    {
        $this->configVersionsLimit = $value;
        return $this;
    }

    public function getExportConfigVersionsLimit()
    {
        return $this->configVersionsLimit;
    }

}