<?php

declare(strict_types=1);

use Keboola\DbWriter\FunctionalTests\DatadirTest;

return function (DatadirTest $test): void {
    // create table with all column types
    $test->connection->exec('DROP TYPE IF EXISTS glasses_enum CASCADE;');
    $test->connection->exec('CREATE TYPE glasses_enum AS ENUM (\'yes\',\'no\', \'sometimes\');');
};
