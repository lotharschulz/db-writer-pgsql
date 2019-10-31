<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests;

use Keboola\DbWriter\Test\BaseTest;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Process\Process;

class FunctionalRowTest extends BaseFunctionalTest
{
    /** @var string $dataDir */
    protected $dataDir = ROOT_PATH . 'tests/data/functionalRow';

    public function setUp(): void
    {
        // cleanup & init
        $this->prepareDataFiles();
        $config = $this->initConfig();
        $writer = $this->getWriter($config['parameters']);
        $writer->drop($config['parameters']['tableId']);
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
    }
}
