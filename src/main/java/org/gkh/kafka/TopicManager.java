package org.gkh.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicManager.class.getName());

    private Properties properties;
    private AdminClient client;

    public TopicManager(Properties p) {
        properties = p;

        LOGGER.debug("Creating admin client");
        client = AdminClient.create(properties);
    }

    public boolean create(String topicName, int partitions, int replication) {
        boolean success = false;
        NewTopic topic = new NewTopic(topicName, partitions, (short) replication);

        List<NewTopic> topics = new ArrayList<>();
        topics.add(topic);

        LOGGER.debug("Creating new topic");
        CreateTopicsResult result = client.createTopics(topics);
        Map<String, KafkaFuture<Void>> values = result.values();
        // Key: topicName, Value: KafkaFuture{value=null,exception=null,done=false}
        // for (Map.Entry<String, KafkaFuture<Void>> entry : values.entrySet()) {
        // System.out.printf("Key: %s, Value: %s", entry.getKey(), entry.getValue());
        // }

        try {
            values.get(topicName).get(15, TimeUnit.SECONDS);
            success = true;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            success = false;
        } finally {
            client.close();
            LOGGER.debug("Admin client closed");
        }

        return success;
    }

    public void delete(String topicName) {
        List<String> list = new ArrayList<>();
        list.add(topicName);

        DeleteTopicsResult result = client.deleteTopics(list);
        LOGGER.debug(result.toString());
    }
}
