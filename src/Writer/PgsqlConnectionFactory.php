<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\DbWriterConfig\Configuration\ValueObject\DatabaseConfig;
use PDO;
use Psr\Log\LoggerInterface;

class PgsqlConnectionFactory
{
    public static function create(DatabaseConfig $databaseConfig, LoggerInterface $logger): PgsqlConnection
    {
        // convert errors to PDOExceptions
        $options = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ];

        $dsn = sprintf(
            'pgsql:host=%s;port=%s;dbname=%s',
            $databaseConfig->getHost(),
            $databaseConfig->hasPort() ? $databaseConfig->getPort() : '5439',
            $databaseConfig->getDatabase(),
        );

        $logger->info(sprintf(
            'Connecting to DSN "%s"...',
            $dsn,
        ), ['options' => $options]);

        return new PgsqlConnection(
            $logger,
            $dsn,
            $databaseConfig->getUser(),
            $databaseConfig->getPassword(),
            $options,
            function (PDO $connection) use ($databaseConfig): void {
                $connection->exec(sprintf(
                    'SET search_path TO "%s";',
                    $databaseConfig->getSchema(),
                ));
            },
        );
    }
}
