<?php

declare(strict_types=1);

namespace Keboola\DbWriter;

use Keboola\DbWriter\Pgsql\Configuration\PgSQLActionConfigRowDefinition;
use Keboola\DbWriter\Pgsql\Configuration\PgSQLConfigDefinition;
use Keboola\DbWriter\Pgsql\Configuration\PgSQLConfigRowDefinition;

class PgSQLApplication extends Application
{
    public function __construct(array $config, Logger $logger)
    {
        $action = !is_null($config['action']) ?: 'run';
        if (isset($config['parameters']['tables'])) {
            $configDefinition = new PgSQLConfigDefinition();
        } else {
            if ($action === 'run') {
                $configDefinition = new PgSQLConfigRowDefinition();
            } else {
                $configDefinition = new PgSQLActionConfigRowDefinition();
            }
        }

        parent::__construct($config, $logger, $configDefinition);
    }
}
