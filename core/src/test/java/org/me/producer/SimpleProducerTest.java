package org.me.producer;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


public class SimpleProducerTest {
    private static Producer<String, Object> producer;
    private static Admin admin;

    @BeforeAll
    static void init() {
        initProducer();
        initAdmin();
        setUpTopic();
    }

    static void initProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaJsonSerializer.class.getName());
        producer = new KafkaProducer<>(props);
    }

    static void initAdmin() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        admin = AdminClient.create(props);
    }

    static void setUpTopic() {
        final String topic = "simple-topic";
        final int partition = 1;
        final short replication = 1;
        admin.createTopics(
                Collections.singleton(
                        new NewTopic(topic, partition, replication)));
    }

    @AfterAll
    static void clearUp() {
        final String topic = "simple-topic";
        admin.deleteTopics(Collections.singleton(topic));
    }

    @Test
    void testSimpleSend() {
        String topic = "simple-topic";
        String message = "Hello World";
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, message);
        producer.send(record, (recordMetadata, e) -> {
            assertNull(e);
            assertEquals(recordMetadata.topic(), topic);
        });
    }

    @Test
    void testSendWithPartition() {
        final String topic = "simple-topic";
        final String key = "foo";
        final String value = "bar";
        final int partition = 0;
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, partition, key, value);
        producer.send(record, (recordMetadata, e) -> {
            assertNull(e);
            assertEquals(recordMetadata.topic(), topic);
            assertEquals(recordMetadata.partition(), partition);
        });
    }

    @Test
    void testSendWithCustomObject() {
        final String topic = "simple-topic";
        final Foo foo = new Foo("foo", 1);

        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, foo);
        producer.send(record, (recordMetadata, e) -> {
            assertNull(e);
            assertEquals(recordMetadata.topic(), topic);
        });
    }

    record Foo(String name, int id) {
    }
}
