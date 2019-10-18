<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 27/10/16
 * Time: 17:20
 */

namespace Keboola\DbWriter\Writer\Pgsql\Tests;

use Keboola\DbWriter\Test\BaseTest;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Process\Process;

class FunctionalTest extends BaseTest
{
    const DRIVER = 'Pgsql';

    protected $dataDir = ROOT_PATH . 'tests/data/functional';

    protected $tmpDataDir = '/tmp/data';

    public function setUp()
    {
        // cleanup & init
        $this->prepareDataFiles();
        $config = $this->initConfig();
        $writer = $this->getWriter($config['parameters']);
        foreach ($config['parameters']['tables'] as $table) {
            $writer->drop($table['dbName']);
        }
    }

    public function testRun()
    {
        $process = $this->runProcess();
        $this->assertEquals(0, $process->getExitCode(), $process->getOutput());
    }

    public function testRunThroughSSH()
    {
        $this->initConfig(function ($config) {
            $config['parameters']['db']['ssh'] = [
                'enabled' => true,
                'keys' => [
                    '#private' => $this->getPrivateKey(),
                    'public' => $this->getPublicKey()
                ],
                'user' => 'root',
                'sshHost' => 'sshproxy',
                'remoteHost' => 'pgsql',
                'remotePort' => '5432',
                'localPort' => '33006',
            ];
            return $config;
        });

        $process = $this->runProcess();
        $this->assertEquals(0, $process->getExitCode(), $process->getOutput());
    }

    public function testTestConnection()
    {
        $this->initConfig(function ($config) {
            $config['action'] = 'testConnection';
            return $config;
        });

        $process = $this->runProcess();
        $this->assertEquals(0, $process->getExitCode());

        $data = json_decode($process->getOutput(), true);
        $this->assertArrayHasKey('status', $data);
        $this->assertEquals('success', $data['status']);
    }

    public function getPrivateKey(): string
    {
        return file_get_contents('/root/.ssh/id_rsa');
    }
    public function getPublicKey(): string
    {
        return file_get_contents('/root/.ssh/id_rsa.pub');
    }

    private function initConfig(callable $callback = null)
    {
        $configPath = $this->dataDir . '/config.json';
        $config = json_decode(file_get_contents($configPath), true);

        $config['parameters']['writer_class'] = self::DRIVER;
        $config['parameters']['db']['user'] = $this->getEnv(self::DRIVER, 'DB_USER', true);
        $config['parameters']['db']['#password'] = $this->getEnv(self::DRIVER, 'DB_PASSWORD', true);
        $config['parameters']['db']['password'] = $this->getEnv(self::DRIVER, 'DB_PASSWORD', true);
        $config['parameters']['db']['host'] = $this->getEnv(self::DRIVER, 'DB_HOST');
        $config['parameters']['db']['port'] = $this->getEnv(self::DRIVER, 'DB_PORT');
        $config['parameters']['db']['database'] = $this->getEnv(self::DRIVER, 'DB_DATABASE');
        $config['parameters']['db']['schema'] = $this->getEnv(self::DRIVER, 'DB_SCHEMA');


        if ($callback !== null) {
            $config = $callback($config);
        }

        $tmpConfigPath = $this->tmpDataDir . '/config.json';
        @unlink($tmpConfigPath);
        file_put_contents($tmpConfigPath, json_encode($config));

        return $config;
    }

    private function prepareDataFiles()
    {
        $fs = new Filesystem();
        $fs->remove($this->tmpDataDir);
        $fs->mkdir($this->tmpDataDir);
        $fs->mkdir($this->tmpDataDir . '/in/tables/');
        $fs->copy(
            $this->dataDir . '/in/tables/simple.csv',
            $this->tmpDataDir . '/in/tables/simple.csv'
        );
        $fs->copy(
            $this->dataDir . '/in/tables/simple_increment.csv',
            $this->tmpDataDir . '/in/tables/simple_increment.csv'
        );
        $fs->copy(
            $this->dataDir . '/in/tables/special.csv',
            $this->tmpDataDir . '/in/tables/special.csv'
        );
    }

    private function runProcess()
    {
        $process = new Process('php ' . ROOT_PATH . 'run.php --data=' . $this->tmpDataDir . ' 2>&1');
        $process->run();

        return $process;
    }
}
