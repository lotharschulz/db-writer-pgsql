#!/bin/bash
docker login -u="$QUAY_USERNAME" -p="$QUAY_PASSWORD" quay.io
docker tag keboola/db-writer-pgsql quay.io/keboola/db-writer-pgsql:$TRAVIS_TAG
docker push quay.io/keboola/db-writer-pgsql:$TRAVIS_TAG