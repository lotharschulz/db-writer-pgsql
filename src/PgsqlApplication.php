<?php

declare(strict_types=1);

namespace Keboola\DbWriter;

use Keboola\Component\Config\BaseConfig;
use Keboola\Component\UserException;
use Keboola\DbWriter\Configuration\PgsqlTableNodesDecorator;
use Keboola\DbWriterConfig\Configuration\ConfigDefinition;
use Keboola\DbWriterConfig\Configuration\ConfigRowDefinition;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class PgsqlApplication extends Application
{
    protected string $writerName = 'Pgsql';

    protected function loadConfig(): void
    {
        $configClass = $this->getConfigClass();
        $configDefinitionClass = $this->getConfigDefinitionClass();

        if (in_array($configDefinitionClass, [ConfigRowDefinition::class, ConfigDefinition::class])) {
            $definition = new $configDefinitionClass(
                null,
                null,
                null,
                new PgsqlTableNodesDecorator(),
            );
        } else {
            $definition = new $configDefinitionClass();
        }

        try {
            /** @var BaseConfig $config */
            $config = new $configClass(
                $this->getRawConfig(),
                $definition,
            );
            $this->config = $config;
        } catch (InvalidConfigurationException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }
    }
}
