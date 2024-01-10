<?php

declare(strict_types=1);

namespace Keboola\DbWriter\FunctionalTests;

use Keboola\Csv\CsvWriter;
use Keboola\Csv\Exception;
use Keboola\Csv\InvalidArgumentException;
use Keboola\DatadirTests\AbstractDatadirTestCase;
use Keboola\DatadirTests\DatadirTestSpecificationInterface;
use Keboola\DbWriter\TraitTests\CloseSshTunnelsTrait;
use Keboola\DbWriter\Writer\PgsqlConnection;
use Keboola\DbWriter\Writer\PgsqlConnectionFactory;
use Keboola\DbWriterConfig\Configuration\ValueObject\DatabaseConfig;
use Psr\Log\Test\TestLogger;
use RuntimeException;
use Symfony\Component\Filesystem\Filesystem;

class DatadirTest extends AbstractDatadirTestCase
{
    use CloseSshTunnelsTrait;

    public PgsqlConnection $connection;

    protected string $testProjectDir;

    public function __construct(
        ?string $name = null,
        array $data = [],
        string $dataName = '',
    ) {
        putenv('SSH_PRIVATE_KEY=' . file_get_contents('/root/.ssh/id_rsa'));
        putenv('SSH_PUBLIC_KEY=' . file_get_contents('/root/.ssh/id_rsa.pub'));
        parent::__construct($name, $data, $dataName);
        $this->connection = PgsqlConnectionFactory::create($this->getDatabaseConfig(), new TestLogger());
    }

    public function setUp(): void
    {
        parent::setUp();
        $this->closeSshTunnels();
        $this->dropTables();

        $this->testProjectDir = $this->getTestFileDir() . '/' . $this->dataName();

        // Load setUp.php file - used to init database state
        $setUpPhpFile = $this->testProjectDir . '/setUp.php';
        if (file_exists($setUpPhpFile)) {
            // Get callback from file and check it
            $initCallback = require $setUpPhpFile;
            if (!is_callable($initCallback)) {
                throw new RuntimeException(sprintf('File "%s" must return callback!', $setUpPhpFile));
            }

            // Invoke callback
            $initCallback($this);
        }
    }

    /**
     * @dataProvider provideDatadirSpecifications
     */
    public function testDatadir(DatadirTestSpecificationInterface $specification): void
    {
        $tempDatadir = $this->getTempDatadir($specification);

        $process = $this->runScript($tempDatadir->getTmpFolder());

        $this->dumpTables($tempDatadir->getTmpFolder());

        $this->assertMatchesSpecification($specification, $process, $tempDatadir->getTmpFolder());
    }

    private function dropTables(): void
    {
        foreach ($this->getTableNames() as $tableName) {
            $this->connection->exec(sprintf(
                'DROP TABLE IF EXISTS %s',
                $this->connection->quoteIdentifier($tableName),
            ));
        }
    }

    /**
     * @throws Exception|InvalidArgumentException
     */
    private function dumpTables(string $tmpFolder): void
    {
        $dumpDir = $tmpFolder . '/out/db-dump';
        $fs = new Filesystem();
        $fs->mkdir($dumpDir);

        foreach ($this->getTableNames() as $tableName) {
            $this->dumpTableData($tableName, $dumpDir);
        }
    }

    private function getTableNames(): array
    {
        $tables = $this->connection->fetchAll(
            sprintf(
                'SELECT table_name FROM information_schema.tables WHERE table_catalog=\'%s\' AND table_schema = \'%s\';',
                getenv('DB_DATABASE'),
                getenv('DB_SCHEMA'),
            ),
        );

        return array_map(function ($item) {
            return $item['table_name'];
        }, $tables);
    }

    /**
     * @throws Exception|InvalidArgumentException
     */
    private function dumpTableData(string $tableName, string $tmpFolder): void
    {
        $csvDumpFile = new CsvWriter(sprintf('%s/%s.csv', $tmpFolder, $tableName));

        $rows = $this->connection->fetchAll(sprintf(
            'SELECT * FROM %s',
            $this->connection->quoteIdentifier($tableName),
        ));
        if ($rows) {
            $csvDumpFile->writeRow(array_keys(current($rows)));
            foreach ($rows as $row) {
                $csvDumpFile->writeRow($row);
            }
        }
    }

    private function getDatabaseConfig(): DatabaseConfig
    {
        $config = [
            'host' => getenv('DB_HOST'),
            'port' => getenv('DB_PORT'),
            'database' => getenv('DB_DATABASE'),
            'schema' => getenv('DB_SCHEMA'),
            'user' => getenv('DB_USER'),
            '#password' => getenv('DB_PASSWORD'),
        ];

        return DatabaseConfig::fromArray($config);
    }
}