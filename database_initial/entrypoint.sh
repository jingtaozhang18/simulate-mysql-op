#!/bin/bash
set -eu
set -o pipefail

mysql \
  -u ${MYSQL_USR_NAME} \
  -h ${MYSQL_SERVER_URL} \
  -P ${MYSQL_SERVER_PORT} \
  -p${MYSQL_USR_PASSWD} < /sqls/initial_sqls.sql
