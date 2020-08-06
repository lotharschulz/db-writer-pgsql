<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Logger;
use Keboola\DbWriter\Writer;
use Keboola\DbWriter\WriterInterface;
use Symfony\Component\Process\Process;

class Pgsql extends Writer implements WriterInterface
{
    /** @var array $allowedTypes */
    private static $allowedTypes = [
        'int', 'smallint', 'integer', 'bigint',
        'decimal', 'numeric', 'real', 'double precision',
        'float4', 'float8',
        'serial', 'bigserial', 'smallserial',
        'money',
        'boolean',
        'char', 'character',
        'varchar', 'character varying', 'text',
        'bytea',
        'date', 'time', 'time with timezone', 'timestamp', 'timestamp with timezone', 'interval',
        'enum',
        'json', 'jsonb',
        // + array variants, see getAllowedTypes()
    ];

    public function __construct(array $dbParams, Logger $logger)
    {
        parent::__construct($dbParams, $logger);
        $this->logger = $logger;
    }

    public function createConnection(array $dbParams): \PDO
    {
        $this->dbParams = $dbParams;

        // convert errors to PDOExceptions
        $options = [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
        ];

        // check params
        foreach (['host', 'database', 'user', 'password', 'schema'] as $r) {
            if (!isset($dbParams[$r])) {
                throw new UserException(sprintf('Parameter %s is missing.', $r));
            }
        }

        $port = isset($dbParams['port']) ? $dbParams['port'] : '5439';
        $dsn = "pgsql:host={$dbParams['host']};port={$port};dbname={$dbParams["database"]}";

        $this->logger->info(
            "Connecting to DSN '" . $dsn . "'...",
            [
                'options' => $options,
            ]
        );

        $pdo = new \PDO($dsn, $dbParams['user'], $dbParams['password'], $options);
        $pdo->exec("SET search_path TO \"{$dbParams["schema"]}\";");

        return $pdo;
    }

    public function isTableValid(array $table): bool
    {
        // TODO: Implement isTableValid() method.

        return true;
    }

    public function drop(string $tableName): void
    {
        $this->reconnectIfDisconnected();
        $stmt = $this->db->prepare(
            'select count(*) 
            from pg_locks l, pg_stat_all_tables t where l.relation=t.relid
            and relname = :tablename
            group by relation;'
        );
        $stmt->execute([$tableName]);
        $locks = $stmt->fetch()[0];
        if ($locks > 0) {
            $this->logger->info("Table \"$tableName\" is locked by $locks transactions, waiting for them to finish");
        }

        $this->execQuery(sprintf('DROP TABLE IF EXISTS %s;', $this->escape($tableName)));
    }

    public function create(array $table): void
    {
        $this->reconnectIfDisconnected();

        $sql = sprintf(
            'CREATE TABLE %s (',
            $this->escape($table['dbName'])
        );

        $columns = array_filter($table['items'], function ($item) {
            return (strtolower($item['type']) !== 'ignore');
        });
        foreach ($columns as $col) {
            $type = $this->getColumnDataTypeSql($col);
            $null = $col['nullable'] ? 'NULL' : 'NOT NULL';
            $default = empty($col['default']) ? '' : "DEFAULT '{$col['default']}'";
            if ($type === 'TEXT') {
                $default = '';
            }
            $sql .= "{$this->escape($col['dbName'])} $type $null $default";
            $sql .= ',';
        }

        if (!empty($table['primaryKey'])) {
            $writer = $this;
            $sql .= PHP_EOL . sprintf(
                'PRIMARY KEY (%s)',
                implode(',', array_map(function ($col) use ($writer) {
                    return $writer->escape($col);
                }, $table['primaryKey']))
            ) . PHP_EOL;

            $sql .= ',';
        }

        $sql = substr($sql, 0, -1);

        $sql .= ');';

        $this->execQuery($sql);
    }

    private function createStage(array $table): void
    {
        $sqlColumns = array_map(function ($col) {
            if (strtolower($col['type']) === 'text') {
                return sprintf(
                    '%s TEXT NULL',
                    $this->escape($col['dbName'])
                );
            } else {
                return sprintf(
                    '%s %s NULL',
                    $this->escape($col['dbName']),
                    $this->getStageColumnDataTypeSql($col)
                );
            }
        }, array_filter($table['items'], function ($item) {
            return (strtolower($item['type']) !== 'ignore');
        }));

        $this->execQuery(sprintf(
            'CREATE TABLE %s (%s)',
            $this->escape($table['dbName']),
            implode(',', $sqlColumns)
        ));
    }

    public function write(CsvFile $csvFile, array $table): void
    {
        $this->logger->info('Using PSQL');

        // create staging table
        $stagingTable = $table;
        $stagingTable['dbName'] = $this->generateTmpName($table['dbName']);
        $this->drop($stagingTable['dbName']);
        $this->createStage($stagingTable);

        $fieldsArr = [];
        foreach ($stagingTable['items'] as $column) {
            $fieldsArr[] = $this->escape($column['dbName']);
        }

        $copyQuery = escapeshellarg(sprintf(
            "\COPY %s (%s) FROM '%s' WITH CSV HEADER DELIMITER ',' QUOTE '\"';",
            $this->escape($this->dbParams['schema']) . '.' . $this->escape($stagingTable['dbName']),
            implode(',', $fieldsArr),
            realpath($csvFile->getPathname())
        ));

        $psqlCommand = sprintf(
            'psql -h %s -p %s -U %s -d %s -w -c %s;',
            $this->dbParams['host'],
            $this->dbParams['port'],
            $this->dbParams['user'],
            $this->dbParams['database'],
            $copyQuery
        );

        $this->logger->info(sprintf("Uploading data into staging table '%s'", $stagingTable['dbName']));

        try {
            $process = new Process($psqlCommand, null, ['PGPASSWORD' => $this->dbParams['password']]);
            $process->setTimeout(null);

            $process->run();

            if ($process->isSuccessful()) {
                $this->logger->info($process->getOutput());
                $this->logger->info(sprintf("Data imported into staging table '%s'", $stagingTable['dbName']));
            } else {
                throw new UserException('Write process failed: ' . $process->getErrorOutput(), 400);
            }

            // move to destination table
            $this->logger->info('Moving to destination table');
            $columns = [];
            foreach ($table['items'] as $col) {
                $type = $this->getColumnDataTypeSql($col);
                $colName = $this->escape($col['dbName']);
                $srcColName = $colName;
                if (!empty($col['nullable'])) {
                    $srcColName = sprintf("NULLIF(%s, '')", $colName);
                }
                $column = sprintf('CAST(%s AS %s) as %s', $srcColName, $type, $colName);
                $columns[] = $column;
            }
            $query = sprintf(
                'INSERT INTO %s SELECT %s FROM %s',
                $this->escape($table['dbName']),
                implode(',', $columns),
                $this->escape($stagingTable['dbName'])
            );
            $this->execQuery($query);

            $this->logger->info(sprintf("Data moved into table '%s'", $table['dbName']));
        } catch (\Throwable $e) {
            $this->drop($stagingTable['dbName']);
            throw $e;
        }

        // drop staging
        $this->drop($stagingTable['dbName']);
    }

    public function upsert(array $table, string $targetTable): void
    {
        $this->reconnectIfDisconnected();

        $sourceTable = $this->escape($table['dbName']);
        $targetTable = $this->escape($targetTable);

        $columns = array_map(
            function ($item) {
                return $this->escape($item['dbName']);
            },
            array_filter($table['items'], function ($item) {
                return strtolower($item['type']) !== 'ignore';
            })
        );

        if (!empty($table['primaryKey'])) {
            // update data
            $joinClauseArr = [];
            foreach ($table['primaryKey'] as $index => $value) {
                $value = $this->escape($value);
                $joinClauseArr[] = "{$targetTable}.{$value}={$sourceTable}.{$value}";
            }
            $joinClause = implode(' AND ', $joinClauseArr);

            $valuesClauseArr = [];
            foreach ($columns as $index => $column) {
                $valuesClauseArr[] = "{$column}={$sourceTable}.{$column}";
            }
            $valuesClause = implode(',', $valuesClauseArr);

            $query = "
                UPDATE {$targetTable}
                SET {$valuesClause}
                FROM {$sourceTable}
                WHERE {$joinClause}
            ";

            $this->execQuery($query);

            // delete updated from temp table
            $query = "
                DELETE FROM {$sourceTable}
                USING {$targetTable}
                WHERE {$joinClause}
            ";

            $this->execQuery($query);
        }

        // insert new data
        $columnsClause = implode(',', $columns);
        $query = "INSERT INTO {$targetTable} ({$columnsClause}) SELECT * FROM {$sourceTable}";
        $this->execQuery($query);

        // drop temp table
        $this->drop($table['dbName']);
    }

    public static function getAllowedTypes(): array
    {
        $allowedTypes = self::$allowedTypes;
        foreach (self::$allowedTypes as $type) {
            // Each type can be used as array
            $allowedTypes[] = $type . '[]';
        }
        return $allowedTypes;
    }

    public function tableExists(string $tableName): bool
    {
        $this->reconnectIfDisconnected();

        $stmt = $this->db->query(sprintf(
            "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
            $this->dbParams['schema'],
            $tableName
        ));
        $res = $stmt->fetchAll();
        return !empty($res);
    }

    private function execQuery(string $query): void
    {
        $this->reconnectIfDisconnected();
        $logQuery = trim(preg_replace('/\s+/', ' ', $query));

        $this->logger->info(sprintf("Executing query '%s'", $logQuery));
        $this->db->exec($query);
    }

    public function showTables(string $dbName): array
    {
        throw new ApplicationException('Method "showTables" not implemented');
    }

    public function tablePrimaryKey(string $tableName): array
    {
        $query = "
            SELECT
                c.column_name
            FROM
                INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS ccu USING (CONSTRAINT_SCHEMA, CONSTRAINT_NAME)
            JOIN INFORMATION_SCHEMA.COLUMNS AS c ON c.TABLE_SCHEMA = tc.CONSTRAINT_SCHEMA
                AND tc.TABLE_NAME = c.TABLE_NAME AND ccu.COLUMN_NAME = c.COLUMN_NAME
            WHERE CONSTRAINT_TYPE = 'PRIMARY KEY' AND tc.TABLE_SCHEMA = '%s' AND tc.TABLE_NAME = '%s';
        ";

        $stmt = $this->db->query(sprintf($query, $this->dbParams['schema'], $tableName));
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        return array_map(function ($row) {
            return $row['column_name'];
        }, $res);
    }

    public function getTableInfo(string $tableName): array
    {
        throw new ApplicationException('Method "getTableInfo" not implemented');
    }


    public function validateTable(array $tableConfig): void
    {
        $this->isTableValid($tableConfig);
    }

    private function escape(string $str): string
    {
        return '"' . $str . '"';
    }

    public function testConnection(): void
    {
        $this->db->query('select current_date')->execute();
    }

    public function generateTmpName(string $tableName): string
    {
        return $tableName . '_temp_' . uniqid();
    }

    private function reconnectIfDisconnected(): void
    {
        try {
            $this->db->query('select current_date')->execute();
        } catch (\PDOException $e) {
            $this->logger->info('Reconnecting to DB');
            $this->db = $this->createConnection($this->dbParams);
        }
    }

    private function getColumnDataTypeSql(array $columnDefinition): string
    {
        $type = strtoupper($columnDefinition['type']);

        if (!empty($columnDefinition['size'])) {
            if (strtoupper($columnDefinition['type']) === 'ENUM') {
                $type = $columnDefinition['size'];
            } else if (strtoupper($columnDefinition['type']) === 'ENUM[]') {
                $type = $columnDefinition['size'] . '[]';
            } else if (preg_match('~\[\]$~', $type)) {
                // For array type must be first size, then []
                // Eg. DECIMAL[](20,10) is not valid, but DECIMAL(20,10)[] is valid
                $type = preg_replace('~\[\]$~', "({$columnDefinition['size']})[]", $type);
            } else {
                $type .= "({$columnDefinition['size']})";
            }
        }

        return $type;
    }

    private function getStageColumnDataTypeSql(array $columnDefinition): string
    {
        $type = strtolower($columnDefinition['type']);
        if (in_array($type, ['text', 'json', 'jsonb']) || strpos($type, '[]') !== false) {
            return 'TEXT';
        } else {
            $isCharacterType = strstr(strtolower($columnDefinition['type']), 'char') !== false;

            return sprintf(
                'VARCHAR(%s)',
                ($isCharacterType && !empty($columnDefinition['size'])) ? $columnDefinition['size'] : '255'
            );
        }
    }
}
