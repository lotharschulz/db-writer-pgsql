FROM keboola/db-writer-pgsql
MAINTAINER Miroslav Cillik <miro@keboola.com>

RUN yum install -y --enablerepo=epel,remi,remi-php56 install \
    php-pear \
    gcc \
    gcc-c++ \
    autoconf \
    automake

RUN yes | pecl install xdebug
RUN echo "zend_extension=/usr/lib64/php/modules/xdebug.so" > /etc/php.d/xdebug.ini

WORKDIR /code

CMD ./code-coverage.sh