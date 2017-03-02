FROM php:5.6-fpm
MAINTAINER Miroslav Cillik <miro@keboola.com>

# Deps
RUN apt-get update
RUN apt-get install -y wget curl make git bzip2 time libzip-dev libssl1.0.0 openssl
RUN apt-get install -y patch unzip libsqlite3-dev gawk freetds-dev subversion
RUN apt-get install -y libpq-dev php5-dev php5-pgsql postgresql postgresql-contrib

# PHP
RUN docker-php-ext-install pdo pdo_pgsql

# CCL
WORKDIR /usr/local/src
RUN svn co http://svn.clozure.com/publicsvn/openmcl/release/1.11/linuxx86/ccl
RUN cp /usr/local/src/ccl/scripts/ccl64 /usr/local/bin/ccl

# PGloader
WORKDIR /opt/src/
RUN git clone https://github.com/dimitri/pgloader.git
WORKDIR /opt/src/pgloader
#RUN mkdir -p build/bin
RUN make CL=ccl DYNSIZE=1024
RUN cp /opt/src/pgloader/build/bin/pgloader /usr/local/bin

# Composer
RUN php -r "copy('https://getcomposer.org/installer', 'composer-setup.php');"
RUN php -r "if (hash_file('SHA384', 'composer-setup.php') === '55d6ead61b29c7bdee5cccfb50076874187bd9f21f65d8991d46ec5cc90518f447387fb9f76ebae1fbbacf329e583e30') { echo 'Installer verified'; } else { echo 'Installer corrupt'; unlink('composer-setup.php'); } echo PHP_EOL;"
RUN php composer-setup.php
RUN php -r "unlink('composer-setup.php');"
RUN cp /opt/src/pgloader/composer.phar /usr/local/bin

# Main
ADD . /code
WORKDIR /code
RUN echo "memory_limit = -1" >> /etc/php.ini
RUN composer.phar selfupdate && composer.phar install --no-interaction

CMD php ./run.php --data=/data

