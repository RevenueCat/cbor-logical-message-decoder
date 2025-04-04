package com.revenuecat.smt.cbordecoder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.ReplaceField;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class CBORDecoder<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {
    private static final Logger LOGGER = LoggerFactory.getLogger(CBORDecoder.class);
    private static final String DEBEZIUM_LOGICAL_DECODING_MESSAGE_KEY = "message";
    private static final String DEBEZIUM_LOGICAL_DECODING_MESSAGE_CONTENT_KEY = "content";
    private static final String SCHEMA_NAME_SUFFIX = ".Envelope";
    private static final String AFTER = "after";
    private static final String OPERATION = "op";
    private static final Object CREATE_CODE = "c";
    private static final String DEBEZIUM_CONNECTOR_POSTGRESQL_MESSAGE_VALUE = "io.debezium.connector.postgresql.MessageValue";

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        return config;
    }

    @Override
    public void configure(final Map<String, ?> configs) {
    }

    public R apply(final R record) {
        // ignore all messages that are not logical decoding messages
        if (!Objects.equals(record.valueSchema().name(), DEBEZIUM_CONNECTOR_POSTGRESQL_MESSAGE_VALUE)) {
            LOGGER.info("Dropping a non-logical decoding message. Message key: \"{}\"", record.key());
            return null;
        }

        Struct originalValue = requireStruct(record.value(), "Retrieve a record value");
        Struct logicalDecodingMessageContent = getLogicalDecodingMessageContent(originalValue);
        R recordWithoutMessageField = removeLogicalDecodingMessageContentField(record);

        final Schema updatedValueSchema = getUpdatedValueSchema(logicalDecodingMessageContent.schema(), recordWithoutMessageField.valueSchema());
        final Struct updatedValue = getUpdatedValue(updatedValueSchema, originalValue, logicalDecodingMessageContent);

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                null,
                // clear prefix of a logical decoding message
                null,
                updatedValueSchema,
                updatedValue,
                record.timestamp(),
                record.headers());
    }

    private Struct getLogicalDecodingMessageContent(Struct valueStruct) {
        Struct logicalDecodingMessageStruct = requireStruct(valueStruct.get(DEBEZIUM_LOGICAL_DECODING_MESSAGE_KEY),
                "Retrieve content of a logical decoding message");

        if (logicalDecodingMessageStruct.schema().field(DEBEZIUM_LOGICAL_DECODING_MESSAGE_CONTENT_KEY).schema().type() != Schema.Type.BYTES) {
            throw new RuntimeException("The content of a logical decoding message is non-binary");
        }

        byte[] logicalDecodingMessageContentBytes = logicalDecodingMessageStruct.getBytes(DEBEZIUM_LOGICAL_DECODING_MESSAGE_CONTENT_KEY);
        return convertLogicalDecodingMessageContentBytesToStruct(logicalDecodingMessageContentBytes);
    }

    private Struct convertLogicalDecodingMessageContentBytesToStruct(byte[] logicalDecodingMessageContent) {
        CBORFactory cborFactory = new CBORFactory();
        ObjectMapper objectMapper = new ObjectMapper(cborFactory);

        try {
            Map<String, Object> data = objectMapper.readValue(logicalDecodingMessageContent, Map.class);

            SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(null);
            for (var entry : data.entrySet()) {
                Schema schemaType = null;
                Object value = entry.getValue();
                if (value instanceof String) schemaType = Schema.OPTIONAL_STRING_SCHEMA;
                if (value instanceof Long) schemaType = Schema.OPTIONAL_INT64_SCHEMA;
                if (value instanceof Integer) schemaType = Schema.OPTIONAL_INT32_SCHEMA;
                if (value instanceof byte[]) schemaType = Schema.BYTES_SCHEMA;
                if (schemaType == null) {
                    throw new RuntimeException("Don't know how to handle field " + entry.getKey() + " of class " + value.getClass());
                }
                LOGGER.debug("Field {} with type {}", entry.getKey(), schemaType);
                schemaBuilder.field(entry.getKey(), schemaType);
            }

            Schema schema = schemaBuilder.build();
            Struct newValue = new Struct(schema);
            for (var entry : data.entrySet()){
                LOGGER.debug("Struct field {} with type {}", entry.getKey(), entry.getValue());
                newValue.put(entry.getKey(), entry.getValue());
            }
            return newValue;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private static <R extends ConnectRecord<R>> ReplaceField<R> dropFieldFromValueDelegate(String field) {
        ReplaceField<R> dropFieldDelegate = new ReplaceField.Value<>();
        Map<String, String> delegateConfig = new HashMap<>();
        delegateConfig.put("exclude", field);
        dropFieldDelegate.configure(delegateConfig);
        return dropFieldDelegate;
    }

    private R removeLogicalDecodingMessageContentField(R record) {
        final ReplaceField<R> dropFieldDelegate = dropFieldFromValueDelegate(DEBEZIUM_LOGICAL_DECODING_MESSAGE_KEY);
        return dropFieldDelegate.apply(record);
    }

    private Schema getUpdatedValueSchema(Schema logicalDecodingMessageContentSchema, Schema debeziumEventSchema) {
        return getSchemaBuilder(logicalDecodingMessageContentSchema, debeziumEventSchema).build();
    }

    private SchemaBuilder getSchemaBuilder(Schema logicalDecodingMessageContentSchema, Schema debeziumEventSchema) {
        // a schema name ending with such a suffix makes the record processable by Outbox SMT
        String schemaName = debeziumEventSchema.name() + SCHEMA_NAME_SUFFIX;

        SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(schemaName);

        for (Field originalSchemaField : debeziumEventSchema.fields()) {
            schemaBuilder.field(originalSchemaField.name(), originalSchemaField.schema());
        }

        schemaBuilder.field(AFTER, logicalDecodingMessageContentSchema);

        return schemaBuilder;
    }

    private Struct getUpdatedValue(Schema updatedValueSchema, Struct originalValue, Struct logicalDecodingMessageContent) {
        final Struct updatedValue = new Struct(updatedValueSchema);

        for (Field field : updatedValueSchema.fields()) {
            Object fieldValue;
            switch (field.name()) {
                case AFTER:
                    fieldValue = logicalDecodingMessageContent;
                    break;
                case OPERATION:
                    // replace the original operation so that a record will look as INSERT event
                    fieldValue = CREATE_CODE;
                    break;
                default:
                    fieldValue = originalValue.get(field);
                    break;
            }
            updatedValue.put(field, fieldValue);
        }
        return updatedValue;
    }

    @Override
    public void close() {
    }

    @Override
    public String version() {
        return "1.0.0";
    }
}
