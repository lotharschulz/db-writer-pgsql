<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests;

use Generator;
use Keboola\DbWriter\TraitTests\BuildExportConfig;
use Keboola\DbWriter\Writer\PgsqlConnectionFactory;
use Keboola\DbWriter\Writer\PgsqlQueryBuilder;
use Keboola\DbWriter\Writer\PgsqlWriteAdapter;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class PerformanceTest extends TestCase
{
    use BuildExportConfig;

    /**
     * @dataProvider performanceDataProvider
     */
    public function testPerformance(int $columns, int $rows, float $expectedTime): void
    {
        $items = [];
        for ($i = 0; $i < $columns; $i++) {
            $items[] = [
                'name' => 'col' . $i,
                'dbName' => 'col' . $i,
                'type' => 'character varying',
                'nullable' => false,
            ];
        }
        $exportConfig = $this->buildExportConfig($items);

        for ($i = 1; $i <= $rows; $i++) {
            file_put_contents(
                $exportConfig->getTableFilePath(),
                implode(',', array_fill(0, $columns, $i)) . "\n",
                FILE_APPEND,
            );
        }

        $logger = new TestLogger();
        $connection = PgsqlConnectionFactory::create($exportConfig->getDatabaseConfig(), $logger);

        $writeAdapter = new PgsqlWriteAdapter(
            $connection,
            new PgsqlQueryBuilder(),
            $logger,
        );

        $writeAdapter->drop($exportConfig->getDbName());
        $writeAdapter->create($exportConfig->getDbName(), false, $exportConfig->getItems());

        $startTime = microtime(true);
        $writeAdapter->writeData($exportConfig->getDbName(), $exportConfig);
        $stopTime = microtime(true);

        $insertedRows = $connection->fetchAll(sprintf(
            'SELECT COUNT(*) FROM %s',
            $connection->quoteIdentifier($exportConfig->getDbName()),
        ));

        $insertedColumns = $connection->fetchAll(sprintf(
            'SELECT COUNT(*) FROM information_schema.columns WHERE table_name = %s',
            $connection->quote($exportConfig->getDbName()),
        ));

        Assert::assertEquals($rows, $insertedRows[0]['count']);
        Assert::assertEquals($columns, $insertedColumns[0]['count']);
        Assert::assertLessThan($expectedTime, round($stopTime-$startTime, 2));
    }

    public function performanceDataProvider(): Generator
    {
        yield '20-1000' => [20, 1000, 1];
        yield '20-10000' => [20, 10000, 1];
        yield '20-1000000' => [20, 1000000, 10];
        yield '1300-10000' => [1300, 10000, 10];
    }
}
