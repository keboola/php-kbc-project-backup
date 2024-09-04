<?php

declare(strict_types=1);

namespace Keboola\ProjectBackup\Tests;

use Keboola\ProjectBackup\NotificationClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranches;
use Throwable;

trait CleanupKbcProject
{
    protected Client $sapiClient;

    private function cleanupKbcProject(): void
    {
        $components = new Components($this->sapiClient);
        foreach ($components->listComponents() as $component) {
            foreach ($component['configurations'] as $configuration) {
                $components->deleteConfiguration($component['id'], $configuration['id']);

                // delete configuration from trash
                $components->deleteConfiguration($component['id'], $configuration['id']);
            }
        }

        // drop linked buckets
        foreach ($this->sapiClient->listBuckets() as $bucket) {
            if (isset($bucket['sourceBucket'])) {
                $this->sapiClient->dropBucket($bucket['id'], ['force' => true]);
            }
        }

        foreach ($this->sapiClient->listBuckets() as $bucket) {
            $this->sapiClient->dropBucket($bucket['id'], ['force' => true]);
        }

        foreach ($this->sapiClient->listTriggers() as $trigger) {
            $this->sapiClient->deleteTrigger($trigger['id']);
        }

        $notificationClient = new NotificationClient(
            $this->sapiClient->getServiceUrl('notification'),
            $this->sapiClient->getTokenString(),
            [
                'backoffMaxTries' => 3,
                'userAgent' => 'Keboola Project Backup',
            ],
        );

        foreach ($notificationClient->listSubscriptions() as $subscription) {
            try {
                $notificationClient->deleteSubscription($subscription['id']);
            } catch (Throwable $e) {
                // ignore
            }
        }

        $devBranches = new DevBranches($this->sapiClient);
        foreach ($devBranches->listBranches() as $branch) {
            if (!$branch['isDefault']) {
                $devBranches->deleteBranch($branch['id']);
            }
        }
    }
}
