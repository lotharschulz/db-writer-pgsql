<?php

declare(strict_types=1);

namespace Keboola\DbWriter\TraitTests;

use Keboola\Csv\CsvWriter;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;
use Keboola\Temp\Temp;
use Symfony\Component\Filesystem\Filesystem;

trait BuildExportConfig
{
    public function buildExportConfig(?array $items = null): ExportConfig
    {
        $tmp = new Temp();
        $fs = new Filesystem();
        $dataDir = $tmp->getTmpFolder();
        if (!$fs->exists($dataDir . '/in/tables/')) {
            $fs->mkdir($dataDir . '/in/tables/');
        }
        $csv = new CsvWriter($dataDir . '/in/tables/test.csv');
        $csv->writeRow(['id', 'name', 'age']);

        return ExportConfig::fromArray(
            [
                'data_dir' => $dataDir,
                'writer_class' => 'Pgsql',
                'dbName' => 'test',
                'tableId' => 'test',
                'primaryKey' => ['id'],
                'db' => [
                    'host' => (string) getenv('DB_HOST'),
                    'port' => (string) getenv('DB_PORT'),
                    'database' => (string) getenv('DB_DATABASE'),
                    'user' => (string) getenv('DB_USER'),
                    '#password' => (string) getenv('DB_PASSWORD'),
                    'schema' => (string) getenv('DB_SCHEMA'),
                ],
                'items' => $items ?? [
                        [
                            'name' => 'id',
                            'dbName' => 'id',
                            'type' => 'integer',
                            'size' => null,
                            'nullable' => false,
                        ],
                        [
                            'name' => 'name',
                            'dbName' => 'name',
                            'type' => 'character varying',
                            'size' => '255',
                            'nullable' => false,
                        ],
                        [
                            'name' => 'age',
                            'dbName' => 'age',
                            'type' => 'integer',
                            'nullable' => false,
                        ],
                    ],
            ],
            [
                [
                    'source' => 'test',
                    'destination' => 'test.csv',
                    'columns' => ['id', 'name', 'age'],
                ],
            ],
        );
    }
}
