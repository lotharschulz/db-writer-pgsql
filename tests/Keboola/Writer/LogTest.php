<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Logger;
use Keboola\DbWriter\Test\BaseTest;
use Keboola\DbWriter\Writer\Pgsql;
use Keboola\DbWriter\WriterFactory;
use Keboola\DbWriter\WriterInterface;
use Monolog\Handler\TestHandler;

class LogTest extends BaseTest
{
    /** @var Pgsql $writer */
    private $writer;

    /** @var array $config */
    private $config;

    /** @var TestHandler $logHandler */
    private $logHandler;

    /** @var Logger $logger */
    private $logger;

    public function setUp(): void
    {
        $this->logHandler = new TestHandler();
        $this->config = $this->initConfig();
        $this->writer = $this->getWriter($this->config['parameters']);

        $tables = $this->config['parameters']['tables'];
        foreach ($tables as $table) {
            $this->writer->drop($table['dbName']);
        }

        $this->writer->getConnection()->query(
            'DROP TYPE IF EXISTS glasses_enum CASCADE'
        );
        $this->writer->getConnection()->query(
            "CREATE TYPE glasses_enum AS ENUM ('yes','no', 'sometimes');"
        );

        $this->logHandler->clear();
    }

    private function initConfig(): array
    {
        $configPath = $this->dataDir . '/config.json';
        $config = json_decode(file_get_contents($configPath), true);

        $config['parameters']['writer_class'] = ucfirst(PgsqlTest::DRIVER);
        $config['parameters']['db']['user'] = $this->getEnv(PgsqlTest::DRIVER . '_DB_USER', true);
        $config['parameters']['db']['#password'] = $this->getEnv(PgsqlTest::DRIVER . '_DB_PASSWORD', true);
        $config['parameters']['db']['password'] = $this->getEnv(PgsqlTest::DRIVER . '_DB_PASSWORD', true);
        $config['parameters']['db']['host'] = $this->getEnv(PgsqlTest::DRIVER . '_DB_HOST');
        $config['parameters']['db']['port'] = $this->getEnv(PgsqlTest::DRIVER . '_DB_PORT');
        $config['parameters']['db']['database'] = $this->getEnv(PgsqlTest::DRIVER . '_DB_DATABASE');
        $config['parameters']['db']['schema'] = $this->getEnv(PgsqlTest::DRIVER . '_DB_SCHEMA');

        return $config;
    }

    protected function getWriter(array $parameters): WriterInterface
    {
        $writerFactory = new WriterFactory($parameters);

        $logger = new Logger(APP_NAME);
        $logger->pushHandler($this->logHandler);

        $this->logger = $logger;

        return $writerFactory->create($logger);
    }

    private function getInputCsv(string $tableId): string
    {
        return sprintf($this->dataDir . '/in/tables/%s.csv', $tableId);
    }

    public function testDropLock(): void
    {
        // simple table
        $table = $this->config['parameters']['tables'][0];

        // create connection for async query
        $dbParams = $this->config['parameters']['db'];

        $dsn = sprintf(
            'host=%s port=%s dbname=%s user=%s password=%s',
            $dbParams['host'],
            $dbParams['port'],
            $dbParams['database'],
            $dbParams['user'],
            str_replace(' ', '\\ ', $dbParams['password'])
        );

        $connection = pg_connect($dsn);
        if (!$connection) {
            $this->fail(sprintf('pg_connect failed'));
        }

        // test lock
        $this->writer->create($table);

        $result = pg_send_query($connection, '
            begin;
            lock simple in EXCLUSIVE mode;
            select pg_sleep(30);
            commit;
        ');

        $this->assertTrue($result, 'send batch query via pg_send_query failed');

        $this->writer->drop($table['dbName']);

        $checkFound = false;
        foreach ($this->logHandler->getRecords() as $logHandler) {
            $this->assertArrayHasKey('message', $logHandler);

            if (strpos($logHandler['message'], $table['dbName']) !== false) {
                $checkFound = true;
            }
        }

        $this->assertTrue($checkFound);
    }

    public function testDropTable(): void
    {
        // simple table
        $table = $this->config['parameters']['tables'][0];

        $this->writer->drop($table['dbName']);

        $dropFound = false;
        foreach ($this->logHandler->getRecords() as $logHandler) {
            $this->assertArrayHasKey('message', $logHandler);

            if (strpos($logHandler['message'], 'DROP TABLE IF EXISTS') === false) {
                continue;
            }

            if (strpos($logHandler['message'], $table['dbName']) !== false) {
                $dropFound = true;
            }
        }

        $this->assertTrue($dropFound);
    }

    public function testRemovePassword(): void
    {
        // simple table
        $table = $this->config['parameters']['tables'][0];
        $table['incremental'] = false;
        $csvFile = new CsvFile($this->getInputCsv($table['tableId']));

        $this->writer->drop($table['dbName']);
        $this->writer->create($table);
        $this->writer->write($csvFile, $table);

        // TARGET destination is not logged everytime
        $logHasTarget = false;
        $passwordFound = false;
        $replaceFound = false;
        foreach ($this->logHandler->getRecords() as $logHandler) {
            $this->assertArrayHasKey('message', $logHandler);

            if (strpos($logHandler['message'], 'TARGET') !== false) {
                $logHasTarget = true;
            }

            if (strpos($logHandler['message'], $this->config['parameters']['db']['#password']) !== false) {
                $passwordFound = true;
            }

            if (strpos($logHandler['message'], '*SECRET*') !== false) {
                $replaceFound = true;
            }
        }

        $this->assertFalse($passwordFound);

        if ($logHasTarget) {
            $this->assertTrue($replaceFound);
            $this->logger->info('Pgloader does not contain TARGET INFO');
        } else {
            $this->logger->info('Pgloader log contains TARGET INFO');
        }
    }
}
