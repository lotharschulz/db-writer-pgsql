<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests;

use Keboola\DbWriter\Test\BaseTest;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Process\Process;

class FunctionalTest extends BaseFunctionalTest
{
    public function setUp(): void
    {
        // cleanup & init
        $this->prepareDataFiles();
        $config = $this->initConfig();
        $writer = $this->getWriter($config['parameters']);
        foreach ($config['parameters']['tables'] as $table) {
            $writer->drop($table['dbName']);
        }
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
                'localPort' => '33006',
            ];
            return $config;
        });

        $process = $this->runProcess();
        $this->assertEquals(0, $process->getExitCode(), $process->getOutput());
    }

    private function prepareDataFiles(): void
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
}
