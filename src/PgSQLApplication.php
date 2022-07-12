<?php

declare(strict_types=1);

namespace Keboola\DbWriter;

use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Pgsql\Configuration\PgSQLActionConfigRowDefinition;
use Keboola\DbWriter\Pgsql\Configuration\PgSQLConfigDefinition;
use Keboola\DbWriter\Pgsql\Configuration\PgSQLConfigRowDefinition;
use Psr\Log\LoggerInterface;
use SplFileInfo;

class PgSQLApplication extends Application
{
    public function __construct(array $config, LoggerInterface $logger)
    {
        $action = $config['action'] ?? 'run';
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

    public function reorderColumns(SplFileInfo $csv, array $items): array
    {
        $csvHeader = explode(';', fgets(fopen($csv->getPathname(), 'r')));
        $reordered = [];
        foreach ($csvHeader as $csvCol) {
            foreach ($items as $item) {
                if ($csvCol === $item['name']) {
                    $reordered[] = $item;
                }
            }
        }

        return $reordered;
    }
}
