#!/usr/bin/env bash
set -e
set +x


if [ -z "$KAFKA_CONNECT_PLUGIN_PATH" ]; then
    export KAFKA_CONNECT_PLUGIN_PATH="${KAFKA_HOME}/connect"
fi

export KAFKA_HEAP_OPTS="-Xms256m -Xmx512m"

# Generate and print the config file
echo "Starting Kafka Connect with configuration:"

$(dirname $0)/kafka_connect_config_generator.sh | tee /tmp/strimzi-connect.properties | sed -e 's/sasl.jaas.config=.*/sasl.jaas.config=[hidden]/g' -e 's/password=.*/password=[hidden]/g'
echo ""

echo "offset.storage.file.filename=/tmp/offsets" >> /tmp/strimzi-connect.properties
set -x

envsubst '$DBHOSTNAME $DBPORT $DBUSER $DBPASSWORD $DBNAME' < /tmp/eventrouter.properties > /tmp/eventrouter_subst.properties

export KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=file:///kafka/config/connect-log4j2.yaml"

# starting Kafka server with final configuration
cat /tmp/strimzi-connect.properties
cat /tmp/eventrouter_subst.properties
exec "${KAFKA_HOME}/bin/connect-standalone.sh" /tmp/strimzi-connect.properties /tmp/eventrouter_subst.properties
