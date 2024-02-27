FROM keboola/db-component-ssh-proxy:latest AS sshproxy
FROM php:7.4-cli

RUN apt-get update && apt-get install -y wget curl make git bzip2 time libzip-dev openssl gnupg lsb-release
RUN apt-get install -y patch unzip libsqlite3-dev gawk freetds-dev subversion

RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && curl -sL https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
    && apt-get update

RUN apt-get install -y libpq-dev postgresql-client --no-install-recommends

# required to bypass https://github.com/debuerreotype/debuerreotype/issues/10
RUN mkdir -p /usr/share/man/man1 /usr/share/man/man7 \
    && apt-get install -y postgresql-contrib --no-install-recommends

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

COPY --from=sshproxy /root/.ssh /root/.ssh
CMD php ./run.php --data=/data
