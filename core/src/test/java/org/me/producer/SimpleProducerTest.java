package org.me.producer;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class SimpleProducerTest {
    private static Producer<String, String> producer;
    private static Admin admin;

    @BeforeAll
    static void initProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(props);
    }

    @BeforeAll
    static void initAdmin() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        admin = AdminClient.create(props);
    }

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        final String topic = "simple-topic";
        final int partition = 1;
        final short replication = 1;
        CreateTopicsResult result = admin.createTopics(
                Collections.singleton(
                        new NewTopic(topic, partition, replication)));
        result.all().get();
    }

    @AfterEach
    void clearUp() throws ExecutionException, InterruptedException {
        final String topic = "simple-topic";
        DeleteTopicsResult result = admin.deleteTopics(Collections.singleton(topic));
        result.all().get();
    }

    @Test
    void testSimpleSend() {
        String topic = "simple-topic";
        String message = "Hello World";
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
        producer.send(record, (recordMetadata, e) -> {
            assertEquals(recordMetadata.topic(), topic);
        });
    }
}
