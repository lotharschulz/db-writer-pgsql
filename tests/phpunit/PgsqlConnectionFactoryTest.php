<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests;

use Keboola\DbWriter\Writer\PgsqlConnection;
use Keboola\DbWriter\Writer\PgsqlConnectionFactory;
use Keboola\DbWriterConfig\Configuration\ValueObject\DatabaseConfig;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class PgsqlConnectionFactoryTest extends TestCase
{
    public function testCreateConnection(): void
    {
        $databaseConfig = new DatabaseConfig(
            (string) getenv('DB_HOST'),
            (string) getenv('DB_PORT'),
            (string) getenv('DB_DATABASE'),
            (string) getenv('DB_USER'),
            (string) getenv('DB_PASSWORD'),
            (string) getenv('DB_SCHEMA'),
            null,
            null,
        );

        $logger = new TestLogger();

        $connection = PgsqlConnectionFactory::create($databaseConfig, $logger);

        self::assertInstanceOf(PgsqlConnection::class, $connection);
        self::assertTrue($logger->hasInfoThatContains('Connecting to DSN'));
        self::assertTrue($logger->hasInfoThatContains('PgSQL server version:'));
    }
}
