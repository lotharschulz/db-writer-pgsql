<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\DbWriterAdapter\Connection\Connection;
use Keboola\DbWriterAdapter\WriteAdapter;
use Keboola\DbWriterConfig\Configuration\ValueObject\DatabaseConfig;

class Pgsql extends BaseWriter
{
    /** @var PgsqlConnection $connection */
    protected Connection $connection;

    protected function createConnection(DatabaseConfig $databaseConfig): Connection
    {
        return PgsqlConnectionFactory::create($databaseConfig, $this->logger);
    }


    protected function createWriteAdapter(): WriteAdapter
    {
        return new PgsqlWriteAdapter(
            $this->connection,
            new PgsqlQueryBuilder(),
            $this->logger,
        );
    }
}
