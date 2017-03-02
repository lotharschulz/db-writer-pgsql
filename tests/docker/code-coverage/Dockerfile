FROM keboola/db-writer-pgsql:latest
MAINTAINER Miroslav Cillik <miro@keboola.com>

RUN pecl install xdebug-2.5.0 && docker-php-ext-enable xdebug

WORKDIR /code

CMD ./code-coverage.sh