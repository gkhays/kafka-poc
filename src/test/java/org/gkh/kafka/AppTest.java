package org.gkh.kafka;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest {

    private static final Logger LOGGER = LogManager.getLogger(AppTest.class.getName());

    private static Properties clientProperties = new Properties();
    private static Properties consumerProperties = new Properties();

    private String testTopic = "test.topic";

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        InputStream in = AppTest.class.getResourceAsStream("/client.properties");
        clientProperties.load(in);

        in = AppTest.class.getResourceAsStream("/consumer.properties");
        consumerProperties.load(in);
    }

    /**
     * Create a topic.
     */
    @Test
    public void testCreateTopic() {
        TopicManager creator = new TopicManager(clientProperties);
        boolean isCreated = creator.create(testTopic, 1, 1);
        assertTrue(isCreated);

        creator.delete(testTopic);
    }

    @Test
    public void testConsumer() {
        String topic = "kafka.poc.command";
        // TopicManager creator = new TopicManager(clientProperties);
        // boolean isCreated = creator.create(topic, 1, 1);
        // assertTrue(isCreated);

        HashMap<String, String> commandMap = new HashMap<>();
        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);

        LOGGER.debug("Creating Kafka consumer...");
        consumer.subscribe(Collections.singletonList("topicName"));// topic
        consumer.seekToEnd(Collections.emptySet());
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

        if (records.count() == 0) {
            LOGGER.debug("No records for topic " + topic);
        }

        records.forEach(record -> {
            LOGGER.debug("Record partition: " + record.partition());
            LOGGER.debug("Record offset: " + record.offset());
            commandMap.put(record.key(), record.value());
        });

        consumer.commitAsync();
        consumer.close();
    }

    public void testQueueConsumer() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        try {
            Properties properties = new Properties();

            properties.put("bootstrap.servers", "localhost:9092");
            properties.put("group.id", "test");
            properties.put("enable.auto.commit", "true");
            properties.put("auto.commit.interval.ms", "1000");
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            String topic = "backblaze_smart";

            LOGGER.debug("Starting subscription to Kafka messages");

            Future<List> messages = executor.submit(new QueueConsumer(topic, properties));
            List<Object> list = messages.get();

            if (list.isEmpty()) {

                System.out.println("Had to make a second call");
                list = messages.get();
            }

            for (Object obj : list) {
                System.out.println(obj.toString());
            }

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        executor.shutdown();
    }
}
