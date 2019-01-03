package com.kiss.kafka.test;

import com.kiss.formation.kafka.avro.Company;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Hello KStream With AVRO!
 * kafka-topics --zookeeper localhost:2181 --create --topic stream-topic-in-avro --partitions 10 --replication-factor 1
 * kafka-topics --zookeeper localhost:2181 --create --topic stream-topic-out-avro --partitions 10 --replication-factor 1
 * <p>
 * kafka-console-producer --broker-list localhost:9092  --topic stream-topic-in-avro
 * kafka-console-consumer --bootstrap-server localhost:9092 --topic stream-topic-out-avro
 */
public class HelloOnStreamWithAvro {

    private Logger logger = LoggerFactory.getLogger(getClass());

    final static String TOPIC_IN = "stream-topic-in-avro";
    final static String TOPIC_OUT = "stream-topic-out-avro";
    private KafkaStreams streams;

    private final String bootstrapServers;
    private final String registryUrl;

    public HelloOnStreamWithAvro(String bootstrapServers, String registryUrl) {
        this.bootstrapServers = bootstrapServers;
        this.registryUrl = registryUrl;
    }

    public void start() {
        Properties config = new Properties();
        // Connection
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, HelloOnStreamWithAvro.class.getSimpleName());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);

        // Serialization
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);


        // When you want to override serdes explicitly/selectively
        final Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                registryUrl);
        final SpecificAvroSerde<Company> companyAvroSerde = new SpecificAvroSerde<>();
        companyAvroSerde.configure(serdeConfig, false);

        // Building stream topology
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Company> source = builder.stream(TOPIC_IN, Consumed.with(Serdes.String(), companyAvroSerde));
        source
                .filter((key, value) -> !"".equals(value))
                .peek((key, value) -> logger.info("__key={}, value={}", key, value))
                .mapValues(value -> value)
                .to(TOPIC_OUT, Produced.with(Serdes.String(), companyAvroSerde));

        final Topology topology = builder.build();
        logger.info("Topology description {}", topology.describe());

        streams = new KafkaStreams(topology, config);
        streams.start();
    }

    public void destroy() {
        streams.close();
    }
}
