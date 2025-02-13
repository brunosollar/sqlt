#!/usr/bin/env bash

docker run --rm \
--name postgres-test \
-e POSTGRES_HOST_AUTH_METHOD=trust \
-p 5432:5432 \
postgres
