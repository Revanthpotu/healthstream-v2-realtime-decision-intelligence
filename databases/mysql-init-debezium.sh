#!/bin/bash
# HealthStream v2 - create the Debezium MySQL user at container init.
# This file is a .sh init script (not .sql) so it can read passwords from
# the container environment instead of embedding them in a tracked file.
# Requires DEBEZIUM_PASSWORD and MYSQL_ROOT_PASSWORD in the mysql service
# environment (see docker-compose.yml and .env.example).
set -e
: "${DEBEZIUM_PASSWORD:?DEBEZIUM_PASSWORD is not set}"
mysql -u root -p"$MYSQL_ROOT_PASSWORD" -e "CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY '$DEBEZIUM_PASSWORD'; GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%'; FLUSH PRIVILEGES;"
