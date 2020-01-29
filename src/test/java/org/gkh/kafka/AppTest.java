package org.gkh.kafka;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest {

    private static final Logger LOGGER = LogManager.getLogger(AppTest.class.getName());

    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue() {
        LOGGER.debug("This is a rigorous test!");
        assertTrue(true);
    }

    @Test

    public void testQueueConsumer() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        try {
            Properties properties = new Properties();

            properties.put("bootstrap.servers", "fast-data-dev.demo.landoop.com:9092");
            properties.put("enable.auto.commit", "true");
            properties.put("auto.commit.interval.ms", "1000");
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            String topic = "";

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
