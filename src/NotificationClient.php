<?php

declare(strict_types=1);

namespace Keboola\ProjectBackup;

use GuzzleHttp\Psr7\Request;
use Keboola\NotificationClient\SubscriptionClient;

class NotificationClient extends SubscriptionClient
{
    public function listSubscriptions(): array
    {
        $request = new Request(
            'GET',
            'project-subscriptions',
            ['Content-type' => 'application/json'],
        );
        return $this->sendRequest($request);
    }

    public function deleteSubscription(string $id): void
    {
        $request = new Request(
            'DELETE',
            'project-subscriptions/' . $id,
            ['Content-type' => 'application/json'],
        );
        $this->sendRequest($request);
    }
}
