<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\DbWriterAdapter\PDO\PdoConnection;
use PDO;
use Throwable;

class PgsqlConnection extends PdoConnection
{
    private ?string $serverVersion = null;

    public const SERVER_VERSION_UNKNOWN = 'unknown';

    protected function connect(): void
    {
        parent::connect();

        // Get server version (only first time)
        if (!$this->serverVersion) {
            try {
                $this->serverVersion = $this->fetchServerVersion();
            } catch (Throwable $e) {
                // ignore if we can't get the server version
            }
            $this->serverVersion = $this->serverVersion ?: self::SERVER_VERSION_UNKNOWN;
            $this->logger->info(sprintf('PgSQL server version: %s', $this->serverVersion));
        }
    }

    public function testConnection(): void
    {
        $this->exec('select current_date', 1);
    }

    private function fetchServerVersion(): ?string
    {
        $stmt = $this->pdo->query('select version();');
        if (!$stmt) {
            return null;
        }
        $serverVersionRaw = $stmt->fetch(PDO::FETCH_COLUMN);
        if (!is_string($serverVersionRaw)) {
            return null;
        }
        preg_match('~^PostgreSQL (\d+.\d+)~', $serverVersionRaw, $m);
        return $m[1] ?? null;
    }

    public function getServerVersion(): string
    {
        return $this->serverVersion ?? self::SERVER_VERSION_UNKNOWN;
    }

    public function quoteIdentifier(string $str): string
    {
        return '"' . str_replace('"', '""', $str) . '"';
    }
}
