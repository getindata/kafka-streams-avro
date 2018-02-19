package serde;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class GenericAvroSerde<T> implements Serde<T> {

    class GenericAvroSerializer implements Serializer<T> {

        private final KafkaAvroSerializer inner;

        public GenericAvroSerializer() {
            this.inner = new KafkaAvroSerializer();
        }
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            this.inner.configure(configs, isKey);
        }

        @Override
        public byte[] serialize(String topic, T data) {
            return this.inner.serialize(topic, data);
        }

        @Override
        public void close() {
            this.inner.close();
        }
    }

    class GenericAvroDeserializer implements Deserializer<T> {

        KafkaAvroDeserializer inner;

        public GenericAvroDeserializer() {
            this.inner = new KafkaAvroDeserializer();
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            this.inner.configure(configs, isKey);
        }

        @Override
        public T deserialize(String topic, byte[] data) {
            NonRecordContainer container = (NonRecordContainer) this.inner.deserialize(topic, data);
            return (T) container.getValue();
        }

        @Override
        public void close() {
            this.inner.close();
        }
    }

    GenericAvroDeserializer deserializer;
    GenericAvroSerializer serializer;

    public GenericAvroSerde() {
        deserializer = new GenericAvroDeserializer();
        serializer = new GenericAvroSerializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        deserializer.configure(configs, isKey);
        serializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        deserializer.close();
        serializer.close();
    }

    @Override
    public Serializer<T> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return deserializer;
    }
}
