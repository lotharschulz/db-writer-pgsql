FROM php:7.1-cli
MAINTAINER Miroslav Cillik <miro@keboola.com>

# Deps
RUN apt-get update
RUN apt-get install -y wget curl make git bzip2 time libzip-dev openssl
RUN apt-get install -y patch unzip libsqlite3-dev gawk freetds-dev subversion
RUN apt-get install -y libpq-dev

# Install psql
  # required to bypass https://github.com/debuerreotype/debuerreotype/issues/10
RUN mkdir -p /usr/share/man/man1 /usr/share/man/man7 \
  && apt-get install -y postgresql postgresql-contrib --no-install-recommends

# PHP
RUN docker-php-ext-install pdo pdo_pgsql pgsql

# Composer
WORKDIR /root
RUN cd \
  && curl -sS https://getcomposer.org/installer | php \
  && ln -s /root/composer.phar /usr/local/bin/composer

# Main
ADD . /code
WORKDIR /code
RUN echo "memory_limit = -1" >> /usr/local/etc/php/php.ini
RUN echo "date.timezone = \"Europe/Prague\"" >> /usr/local/etc/php/php.ini
RUN composer selfupdate && composer install --no-interaction

CMD php ./run.php --data=/data

