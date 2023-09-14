package io.confluent.healthcheck;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    // private final Producer<String, String> producer;
    // final String outTopic;
    static Logger LOG = LoggerFactory.getLogger(Main.class.getName());
    static int partitionCount;
    static String[] payload;
    static Long[] offset;
    static boolean healthy = true;
    static int maxCycle = 5;
    static long cycleInterval = 30000;

    public static Properties loadProperties(String fileName) throws IOException {
        final Properties envProps = new Properties();
        final FileInputStream input = new FileInputStream(fileName);
        envProps.load(input);
        input.close();

        return envProps;
    }

    private static void run() throws Exception {

        final Properties adminProps = Main.loadProperties("configuration/admin.properties");
        final String topic = adminProps.getProperty("health.topic.name");

        try(final AdminClient adminClient = AdminClient.create(adminProps)) {

            scanBrokerNodes(adminClient);
            boolean exist = isTopicExist(adminClient, topic);
            LOG.info("Is the Health Topic exist? " + exist);
            if (exist) {
                scanTopic(adminClient, topic);
            }

        } catch (ExecutionException e) {

            e.printStackTrace();

        } catch (InterruptedException e) {

            e.printStackTrace();
        }

        final Properties producerProps = Main.loadProperties("configuration/producer.properties");

        try (final Producer<String, String> producer = new KafkaProducer<>(producerProps)) {
            List<PartitionInfo> pInfo = producer.partitionsFor(topic);
            partitionCount = pInfo.size();
            long offsetToRead = 0L;
            payload = new String[partitionCount];
            offset = new Long[partitionCount];
            for (int i = 0; i < partitionCount; ++i) {
                payload[i] = usingRandomUUID();
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, i, null, payload[i]);
                Future<RecordMetadata> f = producer.send(record);
                final RecordMetadata recordMetadata = f.get();
                offsetToRead = recordMetadata.offset();
                offset[i] = offsetToRead;
                LOG.info("Produced " + payload[i] + " at partition " + i + " offset " + offset[i]);
            }
            producer.close();

        } catch (Exception e) {

            e.printStackTrace();
        }

        final Properties consumerProps = Main.loadProperties("configuration/consumer.properties");
        try (final Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            
            for (int i = 0; i < partitionCount; ++i) {
                
                TopicPartition partitionToRead = new TopicPartition(topic, i);
                consumer.assign(Arrays.asList(partitionToRead));
                consumer.seek(partitionToRead, offset[i]);
                
                int numberOfMessagesToRead = 1;
                boolean keepOnReading = true;
                int numberOfMessagesReadSoFar = 0;

                while (keepOnReading) {
                    final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
                    for (ConsumerRecord<String, String> record : consumerRecords) {
                        numberOfMessagesReadSoFar += 1;
                        LOG.info("Consumed " + record.value() + " from partition " + record.partition() + " offset " + offset[i]);
                        healthy = (!payload[i].equals(record.value())) ? false : true;
                        if (numberOfMessagesReadSoFar >= numberOfMessagesToRead){
                            keepOnReading = false;
                            break;
                        }
                    }
                }
            }
            consumer.close();

        } catch (Exception e) {

            e.printStackTrace();
        }

        if (healthy)
            LOG.info("Cluster is healthy");
        else
            LOG.info("Cluster is NOT healthy");
    }

    public static void main(String[] args) throws Exception {

        final Properties props = Main.loadProperties("configuration/admin.properties");
        cycleInterval = Long.parseLong(props.getProperty("cycle.interval"));
        maxCycle = Integer.parseInt(props.getProperty("max.cycle"));
        
        int counter = 1;
        while (counter <= maxCycle) {
            LOG.info("Run cycle # " + counter);
            run();
            ++counter;
            Thread.sleep(cycleInterval);
        }
    }
    
    static String usingRandomUUID() {
        UUID randomUUID = UUID.randomUUID();
        return randomUUID.toString().replaceAll("-", "");
    }

    static Collection<Node> scanBrokerNodes(AdminClient adminClient) throws Exception {
        DescribeClusterResult describeClusterResult = adminClient.describeCluster();
        Collection<Node> nodes = describeClusterResult.nodes().get();
        nodes.forEach(node -> {
            LOG.info(node.id() + " | " + node.host() + ":" + node.port());
        });
        return nodes;
    }

    static int getBrokerNodesCount(AdminClient adminClient) throws Exception {
        Collection<Node> nodes = scanBrokerNodes(adminClient);
        return nodes.size();
    }

    static void listTopics(AdminClient ac) throws Exception {
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(false);
        Collection<TopicListing> topics = ac.listTopics(options).listings().get();
        topics.forEach(topic -> {
            LOG.info(topic.name());
        });  
    }

    static boolean isTopicExist(AdminClient ac, String topic) 
    throws InterruptedException, ExecutionException {
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);
        return ac.listTopics(options).names().get().stream().anyMatch(topicName -> topicName.equalsIgnoreCase(topic));
    }

    static void scanTopic(AdminClient ac, String topic) 
    throws InterruptedException, ExecutionException {
        DescribeTopicsResult result = ac.describeTopics(Collections.singleton(topic));
        Map<String, TopicDescription> results = result.allTopicNames().get();
        results.forEach((name, td) -> {
            LOG.info("Partition Count : " + td.partitions().size());
            td.partitions().forEach(p -> {
                LOG.info("Topic " + topic + " - partition " + p.partition() + " at broker " + p.leader().id());
            });
        });
    }

    static DescribeTopicsResult describeTopics(AdminClient ac, Collection<String> topicNames) {
        return ac.describeTopics(topicNames, new DescribeTopicsOptions());
    }
}

