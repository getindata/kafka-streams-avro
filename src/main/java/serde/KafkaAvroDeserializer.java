package serde;

import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class KafkaAvroDeserializer extends AbstractKafkaAvroDeserializer
        implements Deserializer<Object> {

    private boolean isKey;

    /**
     * Constructor used by Kafka consumer.
     */
    public KafkaAvroDeserializer() {

    }

//    public KafkaAvroDeserializer(SchemaRegistryClient client) {
//        schemaRegistry = client;
//    }
//
//    public KafkaAvroDeserializer(SchemaRegistryClient client, Map<String, ?> props) {
//        schemaRegistry = client;
//        configure(deserializerConfig(props));
//    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        configure(new KafkaAvroDeserializerConfig(configs));
    }

    @Override
    public Object deserialize(String s, byte[] bytes) {
        return deserialize(true, s, this.isKey, bytes, null);
    }

    /**
     * Pass a reader schema to get an Avro projection
     */
    public Object deserialize(String s, byte[] bytes, Schema readerSchema) {
        return deserialize(false, s, this.isKey, bytes, readerSchema);
    }

    @Override
    public void close() {

    }
}