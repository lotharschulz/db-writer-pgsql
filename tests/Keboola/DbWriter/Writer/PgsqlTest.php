<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 05/11/15
 * Time: 13:33
 */

namespace Keboola\DbWriter\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Test\BaseTest;

class PgsqlTest extends BaseTest
{
    const DRIVER = 'pgsql';

    /** @var Pgsql */
    private $writer;

    private $config;

    public function setUp()
    {
        $this->config = $this->initConfig();
        $this->writer = $this->getWriter($this->config['parameters']);

        $tables = $this->config['parameters']['tables'];
        foreach ($tables as $table) {
            $this->writer->drop($table['dbName']);
        }

        $this->writer->getConnection()->query(
            "DROP TYPE IF EXISTS glasses_enum CASCADE"
        );
        $this->writer->getConnection()->query(
            "CREATE TYPE glasses_enum AS ENUM ('yes','no', 'sometimes');"
        );
    }

    private function initConfig()
    {
        $configPath = $this->dataDir . '/config.json';
        $config = json_decode(file_get_contents($configPath), true);

        $config['parameters']['writer_class'] = ucfirst(self::DRIVER);
        $config['parameters']['db']['user'] = $this->getEnv(self::DRIVER, 'DB_USER', true);
        $config['parameters']['db']['#password'] = $this->getEnv(self::DRIVER, 'DB_PASSWORD', true);
        $config['parameters']['db']['password'] = $this->getEnv(self::DRIVER, 'DB_PASSWORD', true);
        $config['parameters']['db']['host'] = $this->getEnv(self::DRIVER, 'DB_HOST');
        $config['parameters']['db']['port'] = $this->getEnv(self::DRIVER, 'DB_PORT');
        $config['parameters']['db']['database'] = $this->getEnv(self::DRIVER, 'DB_DATABASE');
        $config['parameters']['db']['schema'] = $this->getEnv(self::DRIVER, 'DB_SCHEMA');

        return $config;
    }

    private function getInputCsv($tableId)
    {
        return sprintf($this->dataDir . "/in/tables/%s.csv", $tableId);
    }

    public function testDrop()
    {
        $conn = $this->writer->getConnection();
        $conn->exec("DROP TABLE IF EXISTS dropMe");
        $conn->exec("CREATE TABLE dropMe (
          id INT PRIMARY KEY,
          firstname VARCHAR(30) NOT NULL,
          lastname VARCHAR(30) NOT NULL)");

        $this->writer->drop("dropMe");

        $stmt = $conn->query("
            SELECT *
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = 'dropMe'
        ");
        $res = $stmt->fetchAll();

        $this->assertEmpty($res);
    }

    public function testCreate()
    {
        $tables = $this->config['parameters']['tables'];

        foreach ($tables as $table) {
            $table['incremental'] = false;
            $this->writer->drop($table['dbName']);
            $this->writer->create($table);
        }

        /** @var \PDO $conn */
        $conn = $this->writer->getConnection();
        $stmt = $conn->query("
            SELECT *
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = '{$tables[0]['dbName']}'
        ");
        $res = $stmt->fetchAll();

        $this->assertEquals('simple', $res[0]['table_name']);

        foreach ($tables as $table) {
            if (empty($table['primaryKey'])) {
                $table['primaryKey'] = [];
            }

            $primaryKey = $this->writer->tablePrimaryKey($table['dbName']);
            $this->assertEquals($table['primaryKey'], $primaryKey);
        }
    }

    public function testWrite()
    {
        // simple table
        $table = $this->config['parameters']['tables'][0];
        $table['incremental'] = false;
        $csvFile = new CsvFile($this->getInputCsv($table['tableId']));

        $this->writer->drop($table['dbName']);
        $this->writer->create($table);
        $this->writer->write($csvFile, $table);

        $conn = $this->writer->getConnection();
        $stmt = $conn->query("SELECT * FROM {$table['dbName']} ORDER BY id ASC");
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(["id","name","glasses"]);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $this->assertFileEquals($this->getInputCsv($table['tableId']), $resFilename);
    }

    public function testWriteNull()
    {
        $tables = array_filter($this->config['parameters']['tables'], function ($table) {
            return ($table['dbName'] === 'simple_null');
        });
        $table = array_pop($tables);
        $csvFile = new CsvFile($this->getInputCsv($table['tableId']));

        $this->writer->drop($table['dbName']);
        $this->writer->create($table);
        $this->writer->write($csvFile, $table);

        $conn = $this->writer->getConnection();
        $stmt = $conn->query("SELECT * FROM {$table['dbName']} ORDER BY id ASC");
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(["id","name","glasses","age"]);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $this->assertFileEquals($this->getInputCsv($table['tableId']), $resFilename);
    }

    public function testWriteEnum()
    {
        $tables = array_filter(
            $this->config['parameters']['tables'],
            function ($table) {
                return ($table['dbName'] === 'simple_enum');
            }
        );
        $table = array_pop($tables);

        $csvFile = new CsvFile($this->getInputCsv($table['tableId']));
        $this->writer->drop($table['dbName']);
        $this->writer->create($table);
        $this->writer->write($csvFile, $table);

        $conn = $this->writer->getConnection();
        $stmt = $conn->query("SELECT * FROM {$table['dbName']} ORDER BY id ASC");
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(["id","name","glasses"]);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $this->assertFileEquals($this->getInputCsv($table['tableId']), $resFilename);
    }

    public function testUpsert()
    {
        $conn = $this->writer->getConnection();
        $tables = $this->config['parameters']['tables'];
        foreach ($tables as $table) {
            $this->writer->drop($table['dbName']);
        }
        $table = $tables[0];
        $targetTable = $table;
        $table['dbName'] .= $table['incremental']?'_temp_' . uniqid():'';

        // first write
        $csvFile = new CsvFile($this->getInputCsv($table['tableId']));
        $targetTable['incremental'] = false;
        $this->writer->create($targetTable);
        $this->writer->write($csvFile, $targetTable);

        // second write (write to temp table, then merge with target table)
        $csvFile2 = new CsvFile($this->getInputCsv($table['tableId'] . "_increment"));
        $this->writer->create($table);
        $this->writer->write($csvFile2, $table);
        $this->writer->upsert($table, $targetTable['dbName']);

        $stmt = $conn->query("SELECT * FROM {$targetTable['dbName']} ORDER BY id ASC");
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(["id", "name", "glasses"]);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $expectedFilename = $this->getInputCsv($table['tableId'] . "_merged");

        $this->assertFileEquals($expectedFilename, $resFilename);
    }

    public function testUpsertMultiPk()
    {
        // exist
        $conn = $this->writer->getConnection();
        $tables = $this->config['parameters']['tables'];

        foreach ($tables as $table) {
            $this->writer->drop($table['dbName']);
        }
        $table = $tables[0];

        $table['tableId'] = 'multi';
        $table['primaryKey'] = ['id', 'name'];

        $targetTable = $table;
        $table['dbName'] .= $table['incremental']?'_temp_' . uniqid():'';

        // first write
        $csvFile = new CsvFile($this->getInputCsv($table['tableId']));
        $targetTable['incremental'] = false;
        $this->writer->create($targetTable);
        $this->writer->write($csvFile, $targetTable);

        // second write (write to temp table, then merge with target table)
        $csvFile2 = new CsvFile($this->getInputCsv($table['tableId'] . "_increment"));
        $this->writer->create($table);
        $this->writer->write($csvFile2, $table);
        $this->writer->upsert($table, $targetTable['dbName']);

        $stmt = $conn->query("SELECT * FROM {$targetTable['dbName']} ORDER BY id ASC");
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(["id", "name", "glasses"]);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $expectedFilename = $this->getInputCsv($table['tableId'] . "_merged");
        $this->assertFileEquals($expectedFilename, $resFilename);

        $primaryKey = $this->writer->tablePrimaryKey($table['dbName']);
        $this->assertEquals($table['primaryKey'], $primaryKey);
    }


    public function testUpsertMultiPkCaseSensitive()
    {
        // exist
        $conn = $this->writer->getConnection();
        $tables = $this->config['parameters']['tables'];

        foreach ($tables as $table) {
            $this->writer->drop($table['dbName']);
            $this->writer->drop(ucfirst($table['dbName']));
        }
        $table = $tables[0];

        $table['tableId'] = 'multi';
        $table['primaryKey'] = ['Id', "Name"];

        $table['dbName'] = ucfirst($table['dbName']);

        $table['items'] = array_map(function ($item) {
            $item['dbName'] =  ucfirst($item['dbName']);
            return $item;
        }, $table['items']);


        $targetTable = $table;
        $table['dbName'] .= $table['incremental']?'_temp_' . uniqid():'';

        // first write
        $csvFile = new CsvFile($this->getInputCsv($table['tableId']));
        $targetTable['incremental'] = false;
        $this->writer->create($targetTable);
        $this->writer->write($csvFile, $targetTable);

        // second write (write to temp table, then merge with target table)
        $csvFile2 = new CsvFile($this->getInputCsv($table['tableId'] . "_increment"));
        $this->writer->create($table);
        $this->writer->write($csvFile2, $table);
        $this->writer->upsert($table, $targetTable['dbName']);

        $stmt = $conn->query("SELECT * FROM \"{$targetTable['dbName']}\" ORDER BY \"Id\" ASC");
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(array_keys(reset($res)));
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $expectedFilename = $this->getInputCsv($table['tableId'] . "_merged_ucfirst");
        $this->assertFileEquals($expectedFilename, $resFilename);

        $primaryKey = $this->writer->tablePrimaryKey($table['dbName']);
        $this->assertEquals($table['primaryKey'], $primaryKey);
    }
}
