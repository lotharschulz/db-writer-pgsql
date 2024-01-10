<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\DbWriterAdapter\Connection\Connection;
use Keboola\DbWriterAdapter\Query\DefaultQueryBuilder;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;
use Keboola\DbWriterConfig\Exception\PropertyNotSetException;

class PgsqlQueryBuilder extends DefaultQueryBuilder
{
    public function lockTableQueryStatement(Connection $connection, string $tableName): string
    {
        $sql = <<<SQL
SELECT count(*) 
FROM pg_locks l, pg_stat_all_tables t 
WHERE l.relation=t.relid AND relname = %s 
GROUP BY relation;
SQL;

        return sprintf(
            $sql,
            $connection->quote($tableName),
        );
    }

    /**
     * @param PgsqlConnection $connection
     * @param ItemConfig[] $items
     * @throws PropertyNotSetException
     */
    public function createQueryStatement(
        Connection $connection,
        string $tableName,
        bool $isTempTable,
        array $items,
        ?array $primaryKeys = null,
    ): string {
        // Table can already exist (incremental load), CREATE TABLE IF NOT EXISTS is supported for PgSQL >= 9.1
        // https://stackoverflow.com/a/7438222
        $createTableStmt =
            $connection->getServerVersion() === PgsqlConnection::SERVER_VERSION_UNKNOWN ||
            version_compare($connection->getServerVersion(), '9.1', 'ge') ?
                'CREATE %s TABLE IF NOT EXISTS' : 'CREATE %s TABLE';

        $createTableStmt = sprintf(
            $createTableStmt,
            $isTempTable ? 'TEMPORARY' : '',
        );

        $filteredItems = array_filter($items, function ($item) {
            return (strtolower($item->getType()) !== 'ignore');
        });

        $columnsDefinition = array_map(
            function (ItemConfig $itemConfig) use ($connection) {
                return sprintf(
                    '%s %s %s %s',
                    $connection->quoteIdentifier($itemConfig->getDbName()),
                    $this->getColumnDataTypeSql($itemConfig),
                    $itemConfig->getNullable() ? 'NULL' : 'NOT NULL',
                    $itemConfig->hasDefault() && $itemConfig->getType() !== 'TEXT' ?
                        'DEFAULT ' . $connection->quote($itemConfig->getDefault()) :
                        '',
                );
            },
            $filteredItems,
        );

        if ($primaryKeys) {
            $columnsDefinition[] = sprintf(
                'PRIMARY KEY (%s)',
                implode(',', array_map(fn($item) => $connection->quoteIdentifier($item), $primaryKeys)),
            );
        }

        return sprintf(
            '%s %s (%s);',
            $createTableStmt,
            $connection->quoteIdentifier($tableName),
            implode(',', $columnsDefinition),
        );
    }

    public function upsertUpdateRowsQueryStatement(
        Connection $connection,
        ExportConfig $exportConfig,
        string $stageTableName,
    ): string {
        $targetTable = $connection->quoteIdentifier($exportConfig->getDbName());
        $sourceTable = $connection->quoteIdentifier($stageTableName);
        $columns = array_map(function ($item) {
            return $item->getDbName();
        }, $exportConfig->getItems());

        // update data
        $joinClauseArr = array_map(fn($item) => sprintf(
            '%s.%s = %s.%s',
            $targetTable,
            $connection->quoteIdentifier($item),
            $sourceTable,
            $connection->quoteIdentifier($item),
        ), $exportConfig->getPrimaryKey());
        $joinClause = implode(' AND ', $joinClauseArr);

        $valuesClauseArr = array_map(fn($item) => sprintf(
            '%s = %s.%s',
            $connection->quoteIdentifier($item),
            $sourceTable,
            $connection->quoteIdentifier($item),
        ), $columns);
        $valuesClause = implode(', ', $valuesClauseArr);

        return sprintf(
            'UPDATE %s SET %s FROM %s WHERE %s',
            $targetTable,
            $valuesClause,
            $sourceTable,
            $joinClause,
        );
    }

    public function upsertDeleteRowsQueryStatement(
        Connection $connection,
        ExportConfig $exportConfig,
        string $stageTableName,
    ): string {
        $joinClauseArr = array_map(fn($item) => sprintf(
            '%s.%s = %s.%s',
            $connection->quoteIdentifier($exportConfig->getDbName()),
            $connection->quoteIdentifier($item),
            $connection->quoteIdentifier($stageTableName),
            $connection->quoteIdentifier($item),
        ), $exportConfig->getPrimaryKey());
        $joinClause = implode(' AND ', $joinClauseArr);

        return sprintf(
            'DELETE FROM %s USING %s WHERE %s',
            $connection->quoteIdentifier($stageTableName),
            $connection->quoteIdentifier($exportConfig->getDbName()),
            $joinClause,
        );
    }

    public function getCopyQueryStatement(
        Connection $connection,
        ExportConfig $exportConfig,
        string $stageTableName,
    ): string {
        return escapeshellarg(sprintf(
            "\COPY %s.%s (%s) FROM '%s' WITH CSV HEADER DELIMITER ',' QUOTE '\"';",
            $connection->quoteIdentifier($exportConfig->getDatabaseConfig()->getSchema()),
            $connection->quoteIdentifier($stageTableName),
            implode(
                ',',
                array_map(fn($item) => $connection->quoteIdentifier($item->getDbName()), $exportConfig->getItems()),
            ),
            realpath($exportConfig->getTableFilePath()),
        ));
    }

    public function writeDataFromStageToTableQueryStatement(
        Connection $connection,
        string $stageTableName,
        string $tableName,
        ExportConfig $exportConfig,
    ): string {
        $items = array_filter($exportConfig->getItems(), fn($item) => strtolower($item->getType()) !== 'ignore');
        $columns = array_map(function (ItemConfig $item) use ($connection) {
            $type = $this->getColumnDataTypeSql($item);
            $colName = $connection->quoteIdentifier($item->getDbName());
            $srcColName = $colName;
            if ($item->getNullable()) {
                $srcColName = sprintf("NULLIF(%s, '')", $colName);
            }
            return sprintf('CAST(%s AS %s) as %s', $srcColName, $type, $colName);
        }, $items);

        return sprintf(
            'INSERT INTO %s SELECT %s FROM %s',
            $connection->quoteIdentifier($tableName),
            implode(',', $columns),
            $connection->quoteIdentifier($stageTableName),
        );
    }

    private function getColumnDataTypeSql(ItemConfig $column): string
    {
        $type = strtoupper($column->getType());

        if ($column->hasSize()) {
            if ($type === 'ENUM') {
                $type = $column->getSize();
            } elseif ($type === 'ENUM[]') {
                $type = $column->getSize() . '[]';
            } elseif (preg_match('~\[\]$~', $type)) {
                // For array type must be first size, then []
                // Eg. DECIMAL[](20,10) is not valid, but DECIMAL(20,10)[] is valid
                $type = preg_replace('~\[\]$~', "({$column->getSize()})[]", $type);
            } else {
                $type .= "({$column->getSize()})";
            }
        }

        return (string) $type;
    }

    public function tableInfoQueryStatement(Connection $connection, string $dbName): string
    {
        return sprintf(
            'SELECT column_name AS "Field", data_type AS "Type" FROM information_schema.columns WHERE table_name = %s',
            $connection->quote($dbName),
        );
    }
}
