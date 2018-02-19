import com.getindata.tutorial.avro.LogEvent;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.val;

import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;


import java.util.Collections;
import java.util.Properties;

public class PlayCount {



    public static void main(String[] args) {
        // Streams Configuration
        val props = new Properties();

        // TODO: change app name
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, YOUR_APP_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "cdh-data-1.gid:9092");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 2 * 1000);


        // Creating Serdes
        val serdeConfiguration =
                Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://cdh-data-1.gid:8081");
        val keyAvroSerde = new serde.StringAvroSerde();
        keyAvroSerde.configure(serdeConfiguration, true);
        val valueAvroSerde = new SpecificAvroSerde<LogEvent>();
        valueAvroSerde.configure(serdeConfiguration, false);

        // Play count by User
        val builder = new StreamsBuilder();

        // TODO: how to get LogEvent class
        // TODO: change input topic
        KStream<String, LogEvent> playStream = builder.stream(YOUR_INPUT_TOPIC, Consumed.with(keyAvroSerde, valueAvroSerde));

        val count = ? // TODO: count stream group by user

        val longAvroSerde = new serde.LongAvroSerde();
        longAvroSerde.configure(serdeConfiguration, false);

        // TODO: change topic
        count.to(YOUR_OUTPUT_TOPIC, Produced.with(keyAvroSerde, longAvroSerde));

        val topology = builder.build();
        System.out.println(topology.describe());

        val streams = new KafkaStreams(topology, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
