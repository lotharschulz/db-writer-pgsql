<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests;

use Keboola\DbWriter\Test\BaseTest;
use Symfony\Component\Process\Process;

abstract class BaseFunctionalTest extends BaseTest
{
    protected const DRIVER = 'Pgsql';

    /** @var string $tmpDataDir */
    protected $tmpDataDir = '/tmp/data';

    /** @var string $dataDir */
    protected $dataDir = ROOT_PATH . 'tests/data/functional';

    public function testRun(): void
    {
        $process = $this->runProcess();
        $this->assertEquals(0, $process->getExitCode(), $process->getErrorOutput());
    }

    public function testTestConnection(): void
    {
        $this->initConfig(function ($config) {
            $config['action'] = 'testConnection';
            return $config;
        });

        $process = $this->runProcess();
        $this->assertEquals(0, $process->getExitCode(), $process->getErrorOutput());

        $data = json_decode($process->getOutput(), true);
        $this->assertArrayHasKey('status', $data);
        $this->assertEquals('success', $data['status']);
    }

    public function testRunThroughSSH(): void
    {
        $this->initConfig(function ($config) {
            $config['parameters']['db']['ssh'] = [
                'enabled' => true,
                'keys' => [
                    '#private' => $this->getPrivateKey(),
                    'public' => $this->getPublicKey(),
                ],
                'user' => 'root',
                'sshHost' => 'sshproxy',
                'remoteHost' => 'pgsql',
                'remotePort' => '5432',
                'localPort' => rand(33000, 33999),
            ];
            return $config;
        });

        $process = $this->runProcess();
        $this->assertEquals(0, $process->getExitCode(), $process->getErrorOutput());
    }

    protected function initConfig(?callable $callback = null): array
    {
        $configPath = $this->dataDir . '/config.json';
        $config = json_decode(file_get_contents($configPath), true);

        $config['parameters']['writer_class'] = self::DRIVER;
        $config['parameters']['db']['user'] = $this->getEnv(self::DRIVER . '_DB_USER', true);
        $config['parameters']['db']['#password'] = $this->getEnv(self::DRIVER . '_DB_PASSWORD', true);
        $config['parameters']['db']['password'] = $this->getEnv(self::DRIVER . '_DB_PASSWORD', true);
        $config['parameters']['db']['host'] = $this->getEnv(self::DRIVER . '_DB_HOST');
        $config['parameters']['db']['port'] = $this->getEnv(self::DRIVER . '_DB_PORT');
        $config['parameters']['db']['database'] = $this->getEnv(self::DRIVER . '_DB_DATABASE');
        $config['parameters']['db']['schema'] = $this->getEnv(self::DRIVER . '_DB_SCHEMA');

        if ($callback !== null) {
            $config = $callback($config);
        }

        $tmpConfigPath = $this->tmpDataDir . '/config.json';
        @unlink($tmpConfigPath);
        file_put_contents($tmpConfigPath, json_encode($config));

        return $config;
    }

    protected function runProcess(): Process
    {
        $process = Process::fromShellCommandline('php ' . ROOT_PATH . 'run.php --data=' . $this->tmpDataDir);
        $process->run();

        return $process;
    }

    public function getPrivateKey(): string
    {
        return file_get_contents('/root/.ssh/id_rsa');
    }

    public function getPublicKey(): string
    {
        return file_get_contents('/root/.ssh/id_rsa.pub');
    }
}
