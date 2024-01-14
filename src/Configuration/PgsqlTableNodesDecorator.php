<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Configuration;

use Keboola\DbWriterConfig\Configuration\NodeDefinition\TableNodesDecorator;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;

class PgsqlTableNodesDecorator extends TableNodesDecorator
{
    protected function addDbNameNode(NodeBuilder $nodeBuilder): void
    {
        $nodeBuilder
            ->scalarNode('dbName')
            ->isRequired()
            ->cannotBeEmpty()
            ->validate()
                ->ifTrue(function ($v) {
                    return strlen($v) > 63;
                })
                ->thenInvalid('PostgreSQL has limit of table name length for 63 characters')
            ->end()
        ;
    }
}
