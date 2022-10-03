<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests;

use Symfony\Component\Filesystem\Filesystem;

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
