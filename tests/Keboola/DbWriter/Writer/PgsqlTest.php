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
        $this->config = $this->getConfig(self::DRIVER);
        $this->config['parameters']['writer_class'] = 'Pgsql';
        $this->config['parameters']['db']['password'] = $this->config['parameters']['db']['#password'];
        $this->writer = $this->getWriter($this->config['parameters']);

        $tables = $this->config['parameters']['tables'];
        foreach ($tables as $table) {
            $this->writer->drop($table['dbName']);
        }
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
    }

    public function testWrite()
    {
        $tables = $this->config['parameters']['tables'];

        // simple table
        $table = $tables[0];
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

    public function testWriteEmpty()
    {
//        $tables = $this->config['parameters']['tables'];
//        $tables = array_filter($tables, function ($table) {
//            return $table['tableId'] === 'empty';
//        });
//
//        // empty table
//        $table = reset($tables);
//
//        $this->writer->drop($table['dbName']);
//        $this->writer->create($table);
//        $this->writer->writeFromS3($s3manifest, $table);
//
//        $conn = $this->writer->getConnection();
//        $stmt = $conn->query("SELECT * FROM {$table['dbName']} ORDER BY id ASC");
//        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);
//
//        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
//        $csv = new CsvFile($resFilename);
//        $csv->writeRow(["id","name"]);
//        foreach ($res as $row) {
//            $csv->writeRow($row);
//        }
//
//        $this->assertFileEquals($this->getInputCsv($table['tableId']), $resFilename);
    }

    public function testUpsert()
    {
//        $conn = $this->writer->getConnection();
//        $tables = $this->config['parameters']['tables'];
//        foreach ($tables as $table) {
//            $this->writer->drop($table['dbName']);
//        }
//        $table = $tables[0];
//
//        $s3Manifest = $this->loadDataToS3($table['tableId']);
//
//        $targetTable = $table;
//        $table['dbName'] .= $table['incremental']?'_temp_' . uniqid():'';
//
//        // first write
//        $this->writer->create($targetTable);
//        $this->writer->writeFromS3($s3Manifest, $targetTable);
//
//        // second write
//        $s3Manifest = $this->loadDataToS3($table['tableId'] . "_increment");
//        $this->writer->create($table);
//        $this->writer->writeFromS3($s3Manifest, $table);
//
//        $this->writer->upsert($table, $targetTable['dbName']);
//
//        $stmt = $conn->query("SELECT * FROM {$targetTable['dbName']} ORDER BY id ASC");
//        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);
//
//        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
//        $csv = new CsvFile($resFilename);
//        $csv->writeRow(["id", "name", "glasses"]);
//        foreach ($res as $row) {
//            $csv->writeRow($row);
//        }
//
//        $expectedFilename = $this->getInputCsv($table['tableId'] . "_merged");
//
//        $this->assertFileEquals($expectedFilename, $resFilename);
    }
}
