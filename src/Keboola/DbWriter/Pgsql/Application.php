<?php
namespace Keboola\DbWriter\Pgsql;

use Keboola\DbWriter\Logger;
use Keboola\DbWriter\Pgsql\Configuration\ConfigDefinition;

class Application extends \Keboola\DbWriter\Application
{
    public function __construct(array $config, Logger $logger)
    {
        parent::__construct($config, $logger, new ConfigDefinition());
    }
}