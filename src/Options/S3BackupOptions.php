<?php
namespace Keboola\ProjectBackup\Options;

class S3BackupOptions
{
    private $targetBucket;

    private $targetBasePath = null;

    private $onlyStructure = false;

    private $configVersions = true;

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

    public function setExportConfigVersions(bool $value)
    {
        $this->configVersions = $value;
        return $this;
    }

    public function getExportConfigVersions()
    {
        return $this->configVersions;
    }

}