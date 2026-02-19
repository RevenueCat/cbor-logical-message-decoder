#!/usr/bin/env bash
set -e

SECURITY_PROTOCOL=PLAINTEXT

# Write the config file
cat <<EOF
# Bootstrap servers
bootstrap.servers=${KAFKA_CONNECT_BOOTSTRAP_SERVERS}
# REST Listeners
rest.port=8083
rest.advertised.host.name=${ADVERTISED_HOSTNAME}
rest.advertised.port=8083
# Plugins
plugin.path=${KAFKA_CONNECT_PLUGIN_PATH}
# Provided configuration
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
group.id=connect
offset.storage.topic=connect-offsets
offset.storage.replication.factor=1
config.storage.topic=connect-configs
config.storage.replication.factor=1
status.storage.topic=connect-status
status.storage.replication.factor=1

${KAFKA_CONNECT_CONFIGURATION}

security.protocol=${SECURITY_PROTOCOL}
producer.security.protocol=${SECURITY_PROTOCOL}
consumer.security.protocol=${SECURITY_PROTOCOL}
admin.security.protocol=${SECURITY_PROTOCOL}

# Additional configuration
consumer.client.rack=${STRIMZI_RACK_ID}
EOF
