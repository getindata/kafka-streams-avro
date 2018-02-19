import com.getindata.tutorial.avro.LogEvent;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.val;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;


import java.util.Collections;
import java.util.Properties;

public class PlayCount {



    public static void main(String[] args) {
        // Streams Configuration
        val props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "lion-play-count4");
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
        KStream<String, LogEvent> playStream = builder.stream("lion-avro", Consumed.with(keyAvroSerde, valueAvroSerde));

        playStream.groupByKey().aggregate(() -> "abc", (key, value, aggregate) -> aggregate);
        val groupedStream = playStream.map((key, value) -> new KeyValue<>(value.getUserid().toString(), 1))
                .groupByKey(Serialized.with(Serdes.String(), Serdes.Integer()));
        val count = groupedStream.count().toStream();
        count.print(Printed.toSysOut());

        val longAvroSerde = new serde.LongAvroSerde();
        longAvroSerde.configure(serdeConfiguration, false);

        count.to("lion-avro-out", Produced.with(keyAvroSerde, longAvroSerde));

        val topology = builder.build();
        System.out.println(topology.describe());

        val streams = new KafkaStreams(topology, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
