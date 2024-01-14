<?php

declare(strict_types=1);

use Keboola\Csv\CsvReader;
use Keboola\DbWriter\FunctionalTests\DatadirTest;

return function (DatadirTest $test): void {
    // create table with all column types
    $test->connection->exec('CREATE TABLE "simple" (
        "Id" INTEGER,
        "Name" VARCHAR(255),
        "glasses" VARCHAR(255)
    )');

    // insert 100 row to table and different all values
    $insert = $test
        ->connection
        ->getConnection()->prepare('INSERT INTO "simple" VALUES (?, ?, ?)');

    $csv = new CsvReader(__DIR__ . '/source/data/in/tables/base.csv');
    $csv->next();
    while ($csv->valid()) {
        $insert->execute((array) $csv->current());
        $csv->next();
    }
};
