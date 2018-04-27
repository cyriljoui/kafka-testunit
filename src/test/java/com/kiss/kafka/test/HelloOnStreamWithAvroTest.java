package com.kiss.kafka.test;


import com.kiss.formation.kafka.avro.Address;
import com.kiss.formation.kafka.avro.Company;
import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.kiss.kafka.test.HelloOnStreamWithAvro.TOPIC_IN;
import static com.kiss.kafka.test.HelloOnStreamWithAvro.TOPIC_OUT;
import static org.junit.Assert.assertTrue;

public class HelloOnStreamWithAvroTest {

    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(TOPIC_IN);
        CLUSTER.createTopic(TOPIC_OUT);
    }

    @Test
    public void testKStream() throws ExecutionException, InterruptedException {
        HelloOnStreamWithAvro helloOnStream = new HelloOnStreamWithAvro(CLUSTER.bootstrapServers(),
                CLUSTER.schemaRegistryUrl());
        helloOnStream.start();

        Company companyFake = Company.newBuilder()
                .setName("FakeOne")
                .setFirstName("Bob")
                .setLastName("Jones")
                .setYearOfCreation(2017)
                .setAddress(
                        Address.newBuilder()
                                .setAddress1("Address1")
                                .setCity("Paris")
                                .setZipCode(75000)
                                .build()).build();

        // Could use convenience methods to produce and consume messages
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl());
        List<Company> inputValues = Arrays.asList(companyFake);
        IntegrationTestUtils.produceValuesSynchronously(TOPIC_IN, inputValues, producerConfig);

        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "avro-standard-consumer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SpecificAvroDeserializer.class);
        consumerConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl());
        List<Company> result = IntegrationTestUtils.waitUntilMinValuesRecordsReceived(consumerConfig,
                TOPIC_OUT, inputValues.size());
        helloOnStream.destroy();

        assertTrue(result.contains(companyFake));
    }
}