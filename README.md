# Postgre SQL DB Writer

[![Build Status](https://travis-ci.org/keboola/db-writer-pgsql.svg?branch=master)](https://travis-ci.org/keboola/db-writer-pgsql)
[![Code Climate](https://codeclimate.com/github/keboola/db-writer-pgsql/badges/gpa.svg)](https://codeclimate.com/github/keboola/db-writer-pgsql)
[![Test Coverage](https://codeclimate.com/github/keboola/db-writer-pgsql/badges/coverage.svg)](https://codeclimate.com/github/keboola/db-writer-pgsql/coverage)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/keboola/db-writer-pgsql/blob/master/LICENSE.md)

Writes data to pgsql Database.

## Example configuration

```json
    {
      "db": {        
        "host": "HOST",
        "port": "PORT",
        "database": "DATABASE",
        "user": "USERNAME",
        "password": "PASSWORD",
        "ssh": {
          "enabled": true,
          "keys": {
            "private": "ENCRYPTED_PRIVATE_SSH_KEY",
            "public": "PUBLIC_SSH_KEY"
          },
          "sshHost": "PROXY_HOSTNAME"
        }
      },
      "tables": [
        {
          "tableId": "simple",
          "dbName": "simple",
          "export": true, 
          "incremental": true,
          "primaryKey": ["id"],
          "items": [
            {
              "name": "id",
              "dbName": "id",
              "type": "int",
              "size": null,
              "nullable": null,
              "default": null
            },
            {
              "name": "name",
              "dbName": "name",
              "type": "nvarchar",
              "size": 255,
              "nullable": null,
              "default": null
            },
            {
              "name": "glasses",
              "dbName": "glasses",
              "type": "nvarchar",
              "size": 255,
              "nullable": null,
              "default": null
            }
          ]                                
        }
      ]
    }
```

## Development

App is developed on localhost using TDD.

1. Clone from repository: `git clone git@github.com:keboola/db-writer-pgsql.git`
2. Change directory: `cd db-writer-pgsql`
3. Create `.env` file with variables:
```
STORAGE_API_TOKEN=
PGSQL_DB_HOST=
PGSQL_DB_PORT=5439
PGSQL_DB_USER=
PGSQL_DB_PASSWORD=
PGSQL_DB_DATABASE=
```
4. Run docker-compose, which will trigger phpunit: `docker-compose run --rm app`
