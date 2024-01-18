<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests;

use Generator;
use Keboola\Csv\CsvWriter;
use Keboola\DbWriter\Writer\PgsqlConnection;
use Keboola\DbWriter\Writer\PgsqlConnectionFactory;
use Keboola\DbWriter\Writer\PgsqlQueryBuilder;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;
use Keboola\Temp\Temp;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;
use Symfony\Component\Filesystem\Filesystem;

class QueryBuilderTest extends TestCase
{
    private PgsqlConnection $connection;

    public function __construct(?string $name = null, array $data = [], $dataName = '')
    {
        $this->connection = PgsqlConnectionFactory::create(
            $this->buildExportConfig()->getDatabaseConfig(),
            new TestLogger(),
        );

        parent::__construct($name, $data, $dataName);
    }

    public function testLockTableQuery(): void
    {
        $expectedQuery = <<<SQL
SELECT count(*) 
FROM pg_locks l, pg_stat_all_tables t 
WHERE l.relation=t.relid AND relname = 'test' 
GROUP BY relation;
SQL;

        $queryBuilder = new PgsqlQueryBuilder();

        $query = $queryBuilder->lockTableQueryStatement($this->connection, 'test');

        Assert::assertEquals($expectedQuery, $query);
    }

    public function testTableInfoQuery(): void
    {
        $expectedQuery = <<<SQL
SELECT column_name AS "Field", data_type AS "Type" FROM information_schema.columns WHERE table_name = 'test'
SQL;

        $queryBuilder = new PgsqlQueryBuilder();

        $query = $queryBuilder->tableInfoQueryStatement($this->connection, 'test');

        Assert::assertEquals($expectedQuery, $query);
    }

    /**
     * @dataProvider createTableQueryDataProvider
     */
    public function testCreateTableQuery(
        bool $tempTable,
        array $items,
        ?array $primaryKeys,
        string $expectedQuery,
    ): void {
        $queryBuilder = new PgsqlQueryBuilder();

        $query = $queryBuilder->createQueryStatement(
            $this->connection,
            'test',
            $tempTable,
            $items,
            $primaryKeys,
        );

        Assert::assertEquals($expectedQuery, $query);
    }

    public function testUpsertUpdateRowsQuery(): void
    {
        // disable phpcs because of long string
        /** @phpcs:disable */
        $expectedQuery = <<<SQL
UPDATE "test" SET "id" = "stageTest"."id", "name" = "stageTest"."name", "age" = "stageTest"."age", "ignoreColumn" = "stageTest"."ignoreColumn" FROM "stageTest" WHERE "test"."id" = "stageTest"."id"
SQL;
        /** @phpcs:enable */

        $queryBuilder = new PgsqlQueryBuilder();

        $query = $queryBuilder->upsertUpdateRowsQueryStatement(
            $this->connection,
            $this->buildExportConfig(),
            'stageTest',
        );

        Assert::assertEquals($expectedQuery, $query);
    }

    public function testUpsertDeleteRowsQuery(): void
    {
        $expectedQuery = <<<SQL
DELETE FROM "stageTest" USING "test" WHERE "test"."id" = "stageTest"."id"
SQL;

        $queryBuilder = new PgsqlQueryBuilder();

        $query = $queryBuilder->upsertDeleteRowsQueryStatement(
            $this->connection,
            $this->buildExportConfig(),
            'stageTest',
        );

        Assert::assertEquals($expectedQuery, $query);
    }

    public function testCopyQuery(): void
    {
        $exportConfig = $this->buildExportConfig();
        // disable phpcs because of long string
        /** @phpcs:disable */
        $expectedQuery = <<<SQL
'\COPY "public"."test" ("id","name","age","ignoreColumn") FROM '\''%s'\'' WITH CSV HEADER DELIMITER '\'','\'' QUOTE '\''"'\'';'
SQL;
        /** @phpcs:enable */

        $expectedQuery = sprintf(
            $expectedQuery,
            $exportConfig->getTableFilePath(),
        );

        $queryBuilder = new PgsqlQueryBuilder();

        $query = $queryBuilder->copyQueryStatement($this->connection, $exportConfig, 'test');

        Assert::assertEquals($expectedQuery, $query);
    }

    public function testWriteDataFromStageToTableQuery(): void
    {
        // disable phpcs because of long string
        /** @phpcs:disable */
        $expectedQuery = <<<SQL
INSERT INTO "test" SELECT CAST("id" AS INT) as "id",CAST("name" AS VARCHAR(255)) as "name",CAST(NULLIF("age", '') AS INT) as "age" FROM "stageName"
SQL;
        /** @phpcs:enable */

        $queryBuilder = new PgsqlQueryBuilder();

        $query = $queryBuilder->writeDataFromStageToTableQueryStatement(
            $this->connection,
            'stageName',
            'test',
            $this->buildExportConfig(),
        );

        Assert::assertEquals($expectedQuery, $query);
    }

    public function createTableQueryDataProvider(): Generator
    {
        $items = [
            ItemConfig::fromArray([
                'name' => 'id',
                'dbName' => 'id',
                'type' => 'int',
                'size' => null,
                'nullable' => false,
            ]),
            ItemConfig::fromArray([
                'name' => 'name',
                'dbName' => 'name',
                'type' => 'varchar',
                'size' => '255',
                'default' => 'test default',
                'nullable' => false,
            ]),
            ItemConfig::fromArray([
                'name' => 'age',
                'dbName' => 'age',
                'type' => 'int',
                'nullable' => true,
            ]),
            ItemConfig::fromArray([
                'name' => 'ignoreColumn',
                'dbName' => 'ignoreColumn',
                'type' => 'ignore',
                'size' => '255',
                'nullable' => false,
            ]),
        ];

        /** @phpcs:disable */
        yield 'simple table' => [
            false,
            $items,
            null,
            'CREATE  TABLE IF NOT EXISTS "test" ("id" INT NOT NULL ,"name" VARCHAR(255) NOT NULL DEFAULT \'test default\',"age" INT NULL );',
        ];

        yield 'simple table with PK' => [
            false,
            $items,
            ['id'],
            'CREATE  TABLE IF NOT EXISTS "test" ("id" INT NOT NULL ,"name" VARCHAR(255) NOT NULL DEFAULT \'test default\',"age" INT NULL ,PRIMARY KEY ("id"));',
        ];

        yield 'simple temp table' => [
            true,
            $items,
            null,
            'CREATE TEMPORARY TABLE IF NOT EXISTS "test" ("id" INT NOT NULL ,"name" VARCHAR(255) NOT NULL DEFAULT \'test default\',"age" INT NULL );',
        ];

        yield 'simple temp table with PK' => [
            true,
            $items,
            ['id'],
            'CREATE TEMPORARY TABLE IF NOT EXISTS "test" ("id" INT NOT NULL ,"name" VARCHAR(255) NOT NULL DEFAULT \'test default\',"age" INT NULL ,PRIMARY KEY ("id"));',
        ];


        /** @phpcs:enable */
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
                'writer_class' => 'MySQL',
                'dbName' => 'test',
                'tableId' => 'test',
                'db' => [
                    'host' => (string) getenv('DB_HOST'),
                    'port' => (string) getenv('DB_PORT'),
                    'database' => (string) getenv('DB_DATABASE'),
                    'user' => (string) getenv('DB_USER'),
                    '#password' => (string) getenv('DB_PASSWORD'),
                    'schema' => (string) getenv('DB_SCHEMA'),
                ],
                'primaryKey' => ['id'],
                'items' => $items ?? [
                        [
                            'name' => 'id',
                            'dbName' => 'id',
                            'type' => 'int',
                            'size' => null,
                            'nullable' => false,
                        ],
                        [
                            'name' => 'name',
                            'dbName' => 'name',
                            'type' => 'varchar',
                            'size' => '255',
                            'nullable' => false,
                        ],
                        [
                            'name' => 'age',
                            'dbName' => 'age',
                            'type' => 'int',
                            'nullable' => true,
                        ],
                        [
                            'name' => 'ignoreColumn',
                            'dbName' => 'ignoreColumn',
                            'type' => 'ignore',
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
