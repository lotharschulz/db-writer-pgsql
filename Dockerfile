FROM quay.io/keboola/docker-base-php56:0.0.2
MAINTAINER Miroslav Cillik <miro@keboola.com>

# Install dependencies
RUN yum -y --enablerepo=epel,remi,remi-php56 install \
    yum-utils \
    rpmdevtools @"Development Tools" \
    sqlite-devel \
    zlib-devel \
    sbcl \
    freetds \
    freetds-devel \
    php-devel \
    php-pgsql \
    postgresql \
    postgresql-contrib; \
    yum clean all

# Install pgloader
WORKDIR /
RUN git clone https://github.com/dimitri/pgloader.git
WORKDIR /pgloader
RUN rpmdev-setuptree
RUN make
RUN cp /pgloader/build/bin/pgloader /usr/local/bin/

ADD . /code
WORKDIR /code

RUN echo "memory_limit = -1" >> /etc/php.ini
RUN composer selfupdate && composer install --no-interaction

CMD php ./run.php --data=/data

