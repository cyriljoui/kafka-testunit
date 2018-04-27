package com.kiss.kafka.test;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Hello KStream!
 * kafka-topics --zookeeper localhost:2181 --create --topic stream-topic-in --partitions 10 --replication-factor 1
 * kafka-topics --zookeeper localhost:2181 --create --topic stream-topic-out --partitions 10 --replication-factor 1
 * <p>
 * kafka-console-producer --broker-list localhost:9092  --topic stream-topic-in
 * kafka-console-consumer --bootstrap-server localhost:9092 --topic stream-topic-out
 */
public class HelloOnStream {

    private Logger logger = LoggerFactory.getLogger(getClass());

    final static String TOPIC_IN = "stream-topic-in";
    final static String TOPIC_OUT = "stream-topic-out";
    private KafkaStreams streams;

    private final String bootstrapServers;

    public HelloOnStream(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public void start() {
        Properties config = new Properties();
        // Connection
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, HelloOnStream.class.getSimpleName());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Serialization
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Building stream topology
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream(TOPIC_IN);
        source
                .filter((key, value) -> !"".equals(value))
                .peek((key, value) -> logger.info("__key={}, value={}", key, value))
                .mapValues(value -> value.toUpperCase())
                .to(TOPIC_OUT);

        final Topology topology = builder.build();
        logger.info("Topology description {}", topology.describe());

        streams = new KafkaStreams(topology, config);
        streams.start();
    }

    public void destroy() {
        streams.close();
    }
}
