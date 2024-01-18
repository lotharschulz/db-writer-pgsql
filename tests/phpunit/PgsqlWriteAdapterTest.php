<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests;

use Keboola\Csv\CsvWriter;
use Keboola\DbWriter\Writer\PgsqlConnectionFactory;
use Keboola\DbWriter\Writer\PgsqlQueryBuilder;
use Keboola\DbWriter\Writer\PgsqlWriteAdapter;
use Keboola\DbWriterAdapter\PDO\PdoConnection;
use Keboola\DbWriterAdapter\Query\QueryBuilder;
use Keboola\DbWriterConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Psr\Log\Test\TestLogger;
use Symfony\Component\Filesystem\Filesystem;
use function PHPUnit\Framework\assertEquals;

class PgsqlWriteAdapterTest extends TestCase
{
    public function __construct(?string $name = null, array $data = [], $dataName = '')
    {
        putenv('COMPONENT_RUN_MODE_RUN=debug');
        parent::__construct($name, $data, $dataName);
    }

    public function testDropTable(): void
    {
        $logger = new TestLogger();
        $exportConfig = $this->buildExportConfig();
        $connection = PgsqlConnectionFactory::create($exportConfig->getDatabaseConfig(), $logger);

        $writeAdapter = new PgsqlWriteAdapter(
            $connection,
            new PgsqlQueryBuilder(),
            $logger,
        );

        $writeAdapter->create($exportConfig->getDbName(), false, $exportConfig->getItems());

        $writeAdapter->drop($exportConfig->getDbName());

        $lockSqlExpected = <<<SQL
SELECT count(*) 
FROM pg_locks l, pg_stat_all_tables t 
WHERE l.relation=t.relid AND relname = 'test' 
GROUP BY relation;
SQL;

        self::assertTrue($logger->hasDebugThatContains(sprintf('Running query "%s".', $lockSqlExpected)));
        self::assertTrue($logger->hasDebugThatContains('Running query "DROP TABLE IF EXISTS "test";".'));
    }

    public function testWriteData(): void
    {
        $logger = new TestLogger();
        $exportConfig = $this->buildExportConfig();
        $connection = PgsqlConnectionFactory::create($exportConfig->getDatabaseConfig(), $logger);

        $writeAdapter = new PgsqlWriteAdapter(
            $connection,
            new PgsqlQueryBuilder(),
            $logger,
        );

        $writeAdapter->create($exportConfig->getDbName(), false, $exportConfig->getItems());
        $writeAdapter->writeData($exportConfig->getDbName(), $exportConfig);

        /** @phpcs:disable */
        // create stage table
        self::assertStringMatchesFormat('Creating staging table "%s"', $logger->records[5]['message']);
        self::assertStringMatchesFormat('Running query "CREATE  TABLE IF NOT EXISTS "%s" ("id" VARCHAR NULL ,"name" VARCHAR NULL ,"age" VARCHAR NULL );".', $logger->records[6]['message']);
        self::assertStringMatchesFormat('Staging table "%s" created', $logger->records[7]['message']);

        // drop and create destination table
        self::assertStringMatchesFormat('Running query "DROP TABLE IF EXISTS "%s";".', $logger->records[9]['message']);
        self::assertStringMatchesFormat('Running query "CREATE  TABLE IF NOT EXISTS "%s" ("id" VARCHAR NULL ,"name" VARCHAR NULL ,"age" VARCHAR NULL );".', $logger->records[10]['message']);

        // write data to stage table
        self::assertStringMatchesFormat('Uploading data into staging table "%s".', $logger->records[11]['message']);
        self::assertStringMatchesFormat('Data imported into staging table "%s".', $logger->records[13]['message']);

        // move data from stage to destination table
        self::assertStringMatchesFormat('Moving to destination table', $logger->records[14]['message']);
        self::assertStringMatchesFormat('Running query "INSERT INTO "test" SELECT CAST("id" AS INTEGER) as "id",CAST("name" AS CHARACTER VARYING(255)) as "name",CAST("age" AS INTEGER) as "age" FROM "%s"".', $logger->records[15]['message']);
        self::assertStringMatchesFormat('Data moved into table "test".', $logger->records[16]['message']);

        // drop stage table
        self::assertStringMatchesFormat('Dropping staging table "%s"', $logger->records[17]['message']);
        self::assertStringMatchesFormat('Running query "DROP TABLE IF EXISTS "%s";".', $logger->records[19]['message']);
        self::assertStringMatchesFormat('Staging table "%s" dropped', $logger->records[20]['message']);
        /** @phpcs:enable */

        self::assertEquals('debug', $logger->records[5]['level']);
        self::assertEquals('debug', $logger->records[6]['level']);
        self::assertEquals('debug', $logger->records[7]['level']);
        self::assertEquals('debug', $logger->records[9]['level']);
        self::assertEquals('debug', $logger->records[10]['level']);
        self::assertEquals('info', $logger->records[11]['level']);
        self::assertEquals('info', $logger->records[13]['level']);
        self::assertEquals('info', $logger->records[14]['level']);
        self::assertEquals('debug', $logger->records[15]['level']);
        self::assertEquals('info', $logger->records[16]['level']);
        self::assertEquals('debug', $logger->records[17]['level']);
        self::assertEquals('debug', $logger->records[19]['level']);
        self::assertEquals('debug', $logger->records[20]['level']);
    }

    private function buildExportConfig(?array $items = null): ExportConfig
    {
        $tmp = new Temp();
        $fs = new Filesystem();
        $dataDir = $tmp->getTmpFolder();
        if (!$fs->exists($dataDir . '/in/tables/')) {
            $fs->mkdir($dataDir . '/in/tables/');
        }
        $csv = new CsvWriter($dataDir . '/in/tables/test.csv');
        $csv->writeRow(['id', 'name', 'age']);

        return ExportConfig::fromArray(
            [
                'data_dir' => $dataDir,
                'writer_class' => 'Pgsql',
                'dbName' => 'test',
                'tableId' => 'test',
                'primaryKey' => ['id'],
                'db' => [
                    'host' => (string) getenv('DB_HOST'),
                    'port' => (string) getenv('DB_PORT'),
                    'database' => (string) getenv('DB_DATABASE'),
                    'user' => (string) getenv('DB_USER'),
                    '#password' => (string) getenv('DB_PASSWORD'),
                    'schema' => (string) getenv('DB_SCHEMA'),
                ],
                'items' => $items ?? [
                        [
                            'name' => 'id',
                            'dbName' => 'id',
                            'type' => 'integer',
                            'size' => null,
                            'nullable' => false,
                        ],
                        [
                            'name' => 'name',
                            'dbName' => 'name',
                            'type' => 'character varying',
                            'size' => '255',
                            'nullable' => false,
                        ],
                        [
                            'name' => 'age',
                            'dbName' => 'age',
                            'type' => 'integer',
                            'nullable' => false,
                        ],
                    ],
            ],
            [
                [
                    'source' => 'test',
                    'destination' => 'test.csv',
                    'columns' => ['id', 'name', 'age'],
                ],
            ],
        );
    }
}
