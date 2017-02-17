<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 12/02/16
 * Time: 16:38
 */

namespace Keboola\DbWriter\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Logger;
use Keboola\DbWriter\Writer;
use Keboola\DbWriter\WriterInterface;
use Symfony\Component\Process\Exception\ProcessFailedException;
use Symfony\Component\Process\Process;

class Pgsql extends Writer implements WriterInterface
{
    private static $allowedTypes = [
        'int', 'int2', 'int4', 'int8',
        'smallint', 'integer', 'bigint',
        'decimal', 'real', 'double precision', 'numeric',
        'float', 'float4', 'float8',
        'boolean',
        'char', 'character', 'nchar', 'bpchar',
        'varchar', 'character varying', 'nvarchar', 'text',
        'date', 'timestamp', 'timestamp without timezone'
    ];

    private static $typesWithSize = [
        'decimal', 'real', 'double precision', 'numeric',
        'float', 'float4', 'float8',
        'char', 'character', 'nchar', 'bpchar',
        'varchar', 'character varying', 'nvarchar', 'text',
    ];

    private $dbParams;

    /** @var Logger */
    protected $logger;

    /** @var \PDO */
    protected $db;

    public function __construct($dbParams, Logger $logger)
    {
        parent::__construct($dbParams, $logger);
        $this->dbParams = $dbParams;
        $this->logger = $logger;
    }

    public function createConnection($dbParams)
    {
        // convert errors to PDOExceptions
        $options = [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION
        ];

        // check params
        foreach (['host', 'database', 'user', 'password', 'schema'] as $r) {
            if (!isset($dbParams[$r])) {
                throw new UserException(sprintf("Parameter %s is missing.", $r));
            }
        }

        $port = isset($dbParams['port']) ? $dbParams['port'] : '5439';
        $dsn = "pgsql:host={$dbParams['host']};port={$port};dbname={$dbParams["database"]}";

        $this->logger->info(
            "Connecting to DSN '" . $dsn . "'...",
            [
                'options' => $options
            ]
        );

        $pdo = new \PDO($dsn, $dbParams['user'], $dbParams['password'], $options);
        $pdo->exec("SET search_path TO \"{$dbParams["schema"]}\";");

        return $pdo;
    }

    public function isTableValid(array $table, $ignoreExport = false)
    {
        // TODO: Implement isTableValid() method.

        return true;
    }

    public function drop($tableName)
    {
        $this->db->exec(sprintf("DROP TABLE IF EXISTS %s;", $this->escape($tableName)));
    }

    public function create(array $table)
    {
        $sql = sprintf(
            "CREATE %s TABLE %s (",
            isset($table['incremental']) && $table['incremental'] ? 'TEMPORARY' : '',
            $this->escape($table['dbName'])
        );

        $columns = array_filter($table['items'], function ($item) {
            return (strtolower($item['type']) !== 'ignore');
        });
        foreach ($columns as $col) {
            $type = strtoupper($col['type']);
            if (!empty($col['size'])) {
                $type .= "({$col['size']})";
            }
            $null = $col['nullable'] ? 'NULL' : 'NOT NULL';
            $default = empty($col['default']) ? '' : "DEFAULT '{$col['default']}'";
            if ($type == 'TEXT') {
                $default = '';
            }
            $sql .= "{$this->escape($col['dbName'])} $type $null $default";
            $sql .= ',';
        }
        $sql = substr($sql, 0, -1);
        $sql .= ");";

        $this->execQuery($sql);
    }

    public function write(CsvFile $csvFile, array $table)
    {
        $copyCommand = sprintf(
            '\copy "%s" FROM \'%s\' WITH CSV HEADER DELIMITER AS \',\'',
            $table['dbName'],
            $csvFile->getPathname()
        );

        $command = sprintf(
            'PGPASSWORD=\'%s\' psql %s -h %s -p %s -U %s -c "%s"',
            $this->dbParams['password'],
            $this->dbParams['database'],
            $this->dbParams['host'],
            $this->dbParams['port'],
            $this->dbParams['user'],
            $copyCommand
        );

        $process = new Process($command);
        $process->setTimeout(null);

        try {
            $process->mustRun();
        } catch (ProcessFailedException $e) {
            throw new UserException("Write process failed: " . $e->getMessage(), 400, $e);
        }
    }

    public function upsert(array $table, $targetTable)
    {
        $sourceTable = $this->escape($table['dbName']);
        $targetTable = $this->escape($targetTable);

        $columns = array_map(
            function ($item) {
                return $this->escape($item['dbName']);
            },
            array_filter($table['items'], function ($item) {
                return strtolower($item['type']) != 'ignore';
            })
        );

        if (!empty($table['primaryKey'])) {
            // update data
            $joinClauseArr = [];
            foreach ($table['primaryKey'] as $index => $value) {
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

    public static function getAllowedTypes()
    {
        return self::$allowedTypes;
    }

    public function tableExists($tableName)
    {
        $stmt = $this->db->query(sprintf("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '%s'", $tableName));
        $res = $stmt->fetchAll();
        return !empty($res);
    }

    private function execQuery($query)
    {
        $this->logger->info(sprintf("Executing query '%s'", $query));
        $this->db->exec($query);
    }

    public function showTables($dbName)
    {
        throw new ApplicationException("Method not implemented");
    }

    public function getTableInfo($tableName)
    {
        throw new ApplicationException("Method not implemented");
    }

    private function escape($str)
    {
        return '"' . $str . '"';
    }

    public function testConnection()
    {
        $this->db->query('select current_date')->execute();
    }

    public function generateTmpName($tableName)
    {
        return $tableName . '_temp_' . uniqid();
    }
}
