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
        'json', 'jsonb'
    ];

    private $dbParams;

    /** @var Logger */
    protected $logger;

    /** @var \PDO */
    protected $db;

    public function __construct($dbParams, Logger $logger)
    {
        parent::__construct($dbParams, $logger);
        $this->logger = $logger;
    }

    public function createConnection($dbParams)
    {
        $this->dbParams = $dbParams;

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

    public function isTableValid(array $table)
    {
        // TODO: Implement isTableValid() method.

        return true;
    }

    public function drop($tableName)
    {
        $this->reconnectIfDisconnected();

        $this->db->exec(sprintf("DROP TABLE IF EXISTS %s;", $this->escape($tableName)));
    }

    public function create(array $table)
    {
        $this->reconnectIfDisconnected();

        $sql = sprintf(
            "CREATE TABLE %s (",
            $this->escape($table['dbName'])
        );

        $columns = array_filter($table['items'], function ($item) {
            return (strtolower($item['type']) !== 'ignore');
        });
        foreach ($columns as $col) {
            $type = strtoupper($col['type']);
            if (!empty($col['size'])) {
                $type .= "({$col['size']})";
                if (strtoupper($col['type']) === 'ENUM') {
                    $type = $col['size'];
                }
            }
            $null = $col['nullable'] ? 'NULL' : 'NOT NULL';
            $default = empty($col['default']) ? '' : "DEFAULT '{$col['default']}'";
            if ($type == 'TEXT') {
                $default = '';
            }
            $sql .= "{$this->escape($col['dbName'])} $type $null $default";
            $sql .= ',';
        }

		if (!empty($table['primaryKey'])) {
			$writer = $this;
			$sql .= PHP_EOL . sprintf(
					"PRIMARY KEY (%s)",
					implode(',', array_map(function($col) use ($writer) {
						return $writer->escape($col);
					}, $table['primaryKey']))
				) . PHP_EOL;

			$sql .= ',';
		}

        $sql = substr($sql, 0, -1);

        $sql .= ");";

        $this->execQuery($sql);
    }

    public function write(CsvFile $csvFile, array $table)
    {
        $fieldsArr = [];
        foreach ($table['items'] as $column) {
            $field = $column['dbName'];
            if (boolval($column['nullable'])) {
                $field .= ' [null if blanks]';
            }
            $fieldsArr[] = $field;
        }

        $connectionString = sprintf(
            'postgres://%s:"%s"@%s:%s/%s?tablename=%s',
            $this->dbParams['user'],
            $this->dbParams['password'],
            $this->dbParams['host'],
            $this->dbParams['port'],
            $this->dbParams['database'],
            $this->escape($this->dbParams['schema']) . '.' . $this->escape($table['dbName'])
        );

        $pgloaderCommand = sprintf(
            'pgloader --debug \
                --client-min-messages debug \
                --type csv \
                --field "%s" \
                --with truncate \
                --with "skip header = 1" \
                --with "fields terminated by \',\'" \
                --with "batch rows = 50000" \
                %s %s',
            implode(',', $fieldsArr),
            realpath($csvFile->getPathname()),
            $connectionString
        );

        $this->logger->info(sprintf("Uploading data into table '%s'", $table['dbName']));
        $process = new Process($pgloaderCommand);
        $process->setTimeout(null);

        try {
            $process->mustRun(function ($type, $buffer) {
                $this->logger->info("pgloader: " . $type . " > " . $buffer);
            });
        } catch (ProcessFailedException $e) {
            throw new UserException("Write process failed: " . $e->getMessage(), 400, $e);
        }
    }

    public function upsert(array $table, $targetTable)
    {
        $this->reconnectIfDisconnected();

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
        $this->reconnectIfDisconnected();

        $stmt = $this->db->query(sprintf("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '%s'", $tableName));
        $res = $stmt->fetchAll();
        return !empty($res);
    }

    private function execQuery($query)
    {
        $this->reconnectIfDisconnected();
		$logQuery = trim(preg_replace('/\s+/', ' ', $query));

        $this->logger->info(sprintf("Executing query '%s'", $logQuery));
        $this->db->exec($query);
    }

    public function showTables($dbName)
    {
        throw new ApplicationException("Method not implemented");
    }

    public function tablePrimaryKey($tableName)
    {
        $query = "
            SELECT
                c.column_name
            FROM
                INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS ccu USING (CONSTRAINT_SCHEMA, CONSTRAINT_NAME)
            JOIN INFORMATION_SCHEMA.COLUMNS AS c ON c.TABLE_SCHEMA = tc.CONSTRAINT_SCHEMA
                AND tc.TABLE_NAME = c.TABLE_NAME AND ccu.COLUMN_NAME = c.COLUMN_NAME
            WHERE CONSTRAINT_TYPE = 'PRIMARY KEY' AND tc.TABLE_NAME = '%s';
        ";

        $stmt = $this->db->query(sprintf($query, $tableName));
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        return array_map(function($row) {
            return $row['column_name'];
        }, $res);
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

    private function reconnectIfDisconnected()
    {
        try {
            $this->logger->info("Test if connection active");
            $this->db->query('select current_date')->execute();
        } catch (\PDOException $e) {
            $this->logger->info("Reconnecting to DB");
            $this->db = $this->createConnection($this->dbParams);
        }
    }
}
