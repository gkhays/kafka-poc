package org.gkh.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueConsumer implements Callable<List> {

    static final Logger LOGGER = LoggerFactory.getLogger(QueueConsumer.class.getName());

    private String topic;
    Properties consumerProps;

    public QueueConsumer(String t, Properties p) {
        consumerProps = p;
        topic = t;
    }

    @Override
    public List<Object> call() {
        KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(topic));

        ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(5000));
        for (ConsumerRecord<Long, String> record : records) {
            LOGGER.debug(String.format("Kafka Message: offset = %d, key = %s, value = %s, partition = %s",
                    record.offset(), record.key(), record.value(), record.partition()));
        }
        consumer.close();
        return null;
    }

}