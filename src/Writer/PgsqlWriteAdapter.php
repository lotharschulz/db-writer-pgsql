<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriterAdapter\PDO\PdoWriteAdapter;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;
use Keboola\DbWriterConfig\Exception\PropertyNotSetException;
use Symfony\Component\Process\Process;

/**
 * @property PgsqlQueryBuilder $queryBuilder
 * @property PgsqlConnection $connection
 */
class PgsqlWriteAdapter extends PdoWriteAdapter
{

    public function drop(string $tableName): void
    {
        $locks = $this->connection->fetchAll(
            $this->queryBuilder->lockTableQueryStatement($this->connection, $tableName),
        );

        if (count($locks) > 0) {
            $this->logger->info(sprintf(
                'Table "%s" is locked by $locks transactions, waiting for them to finish',
                $tableName,
            ));
        }

        $this->connection->exec(
            $this->queryBuilder->dropQueryStatement($this->connection, $tableName),
        );
    }

    public function writeData(string $tableName, ExportConfig $exportConfig): void
    {
        $stageTableName = $this->generateTmpName($tableName);

        $this->createStage($stageTableName, $exportConfig->getItems());

        try {
            $this->writeDataToStage($stageTableName, $exportConfig);
            $this->moveDataFromStageToTable($stageTableName, $tableName, $exportConfig);
        } finally {
            $this->drop($stageTableName);
        }
    }

    /**
     * @param ItemConfig[] $items
     * @throws PropertyNotSetException
     */
    private function createStage(string $stageTableName, array $items): void
    {
        $stageItems = [];
        foreach ($items as $item) {
            if (strtolower($item->getType()) === 'text') {
                $stageItems[] = ItemConfig::fromArray([
                    'name' => $item->getName(),
                    'dbName' => $item->getDbName(),
                    'type' => 'TEXT',
                    'nullable' => true,
                ]);
            } else {
                $type = strtolower($item->getType());
                $size = (str_contains($type, 'char') && $item->hasSize()) ? $item->getSize() : '255';
                $stageItems[] = ItemConfig::fromArray([
                    'name' => $item->getName(),
                    'dbName' => $item->getDbName(),
                    'type' => $this->getStageColumnDataTypeSql($item),
                    'nullable' => true,
                    'size' => $size,
                ]);
            }
        }

        $this->connection->exec(
            $this->queryBuilder->createQueryStatement(
                $this->connection,
                $stageTableName,
                false,
                $stageItems,
            ),
        );
    }

    private function getStageColumnDataTypeSql(ItemConfig $column): string
    {
        $type = strtolower($column->getType());
        if (in_array($type, ['text', 'json', 'jsonb']) || str_contains($type, '[]')) {
            return 'TEXT';
        } else {
            return 'VARCHAR';
        }
    }

    /**
     * @throws PropertyNotSetException
     * @throws UserException
     */
    private function writeDataToStage(string $stageTableName, ExportConfig $exportConfig): void
    {
        $this->drop($stageTableName);
        $this->createStage($stageTableName, $exportConfig->getItems());

        $copyQuery = $this->queryBuilder->getCopyQueryStatement(
            $this->connection,
            $exportConfig,
            $stageTableName,
        );

        $psqlCommand = sprintf(
            'psql -h %s -p %s -U %s -d %s -w -c %s;',
            $exportConfig->getDatabaseConfig()->getHost(),
            $exportConfig->getDatabaseConfig()->getPort(),
            $exportConfig->getDatabaseConfig()->getUser(),
            $exportConfig->getDatabaseConfig()->getDatabase(),
            $copyQuery,
        );

        $this->logger->info(sprintf("Uploading data into staging table '%s'", $stageTableName));

        $process = Process::fromShellCommandline(
            $psqlCommand,
            null,
            ['PGPASSWORD' => $exportConfig->getDatabaseConfig()->getPassword()],
        );
        $process->setTimeout(null);

        $process->run();

        if ($process->isSuccessful()) {
            $this->logger->info($process->getOutput());
            $this->logger->info(sprintf("Data imported into staging table '%s'", $stageTableName));
        } else {
            throw new UserException('Write process failed: ' . $process->getErrorOutput(), 400);
        }
    }

    private function moveDataFromStageToTable(
        string $stageTableName,
        string $tableName,
        ExportConfig $exportConfig,
    ): void {
        $this->logger->info('Moving to destination table');

        $query = $this->queryBuilder->writeDataFromStageToTableQueryStatement(
            $this->connection,
            $stageTableName,
            $tableName,
            $exportConfig,
        );
        $this->connection->exec($query);

        $this->logger->info(sprintf("Data moved into table '%s'", $exportConfig->getDbName()));
    }
}
