FROM quay.io/debezium/connect:3.4.1.Final

ARG DEBEZIUM_CONNECTOR_VERSION="3.0.7.Final"
ARG STRIMZI_VERSION="latest-kafka-3.5.1"

USER 0

RUN yum install -y envsubst

ADD cbordecoder-1.0.jar /kafka/libs
ADD kafka_connect_standalone_run.sh /kafka/bin/kafka_connect_standalone_run.sh
ADD kafka_connect_config_generator.sh /kafka/bin/kafka_connect_config_generator.sh
ADD eventrouter.properties /tmp/eventrouter.properties
ADD log4j2.yaml /kafka/config/connect-log4j2.yaml
RUN rm -f /kafka/config/log4j.properties

CMD /kafka/bin/kafka_connect_standalone_run.sh

