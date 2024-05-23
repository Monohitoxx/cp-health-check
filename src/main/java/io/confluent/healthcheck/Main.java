package io.confluent.healthcheck;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
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
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    static Logger LOG = LoggerFactory.getLogger(Main.class.getName());
    static int partitionCount;
    static String[] payload;
    static Long[] offset;
    static boolean healthy = false;
    static int maxCycle = 5;
    static long cycleInterval = 30000;
    static boolean infiniteRun = true;
    static DateFormat sdf = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");

    public static Properties loadProperties(String fileName) throws IOException {
        final Properties envProps = new Properties();
        final FileInputStream input = new FileInputStream(fileName);
        envProps.load(input);
        input.close();

        return envProps;
    }

    static void scanBrokers(Properties prop) {

        String brokers = prop.getProperty("bootstrap.servers");
        List<String> brokerList = Arrays.asList(brokers.split(","));
        brokerList.forEach(broker -> {
            prop.setProperty("bootstrap.servers", broker);
            try(AdminClient ac = AdminClient.create(prop)) {
                DescribeClusterResult describeClusterResult = ac.describeCluster();
                describeClusterResult.nodes().get();
                // List<Integer> nodeId =  nodes.stream().map(Node::id).collect(Collectors.toList());
                // nodeId.forEach(id -> LOG.info(id + " | " + b + " | online"));
                LOG.info(broker + " | active");
            } catch (Exception e) {
                LOG.info(broker + " | inactive");
                healthy = false;  // Set cluster health to false if any broker is inactive
            }
        });
    }

    private static void run(Properties adminProps) {

        // final Properties adminProps = Main.loadProperties("configuration/admin.properties");
        boolean exist = false;
        final String topic = adminProps.getProperty("health.topic.name");
        
        try(final AdminClient adminClient = AdminClient.create(adminProps)) {
            Collection<Node> nodes = scanBrokerNodes(adminClient);
            exist = isTopicExist(adminClient, topic);
            LOG.info("Is the Health Topic exist? " + exist);
            if (exist) {
                scanTopic(adminClient, topic);
                String mir = getTopicConfig(adminClient, topic, "min.insync.replicas");
                if (nodes.size() < Integer.parseInt(mir)) {
                    LOG.info("Cluster is NOT HEALTHY");
                    return;
                }
            }

        } catch (Exception e) {

            e.printStackTrace();
        }
        
        if (!exist) {
            LOG.info("Health topic is not exist, skipping producer and consumer test");
            return;
        }
        
        Properties producerProps = new Properties();
        adminProps.forEach((k,v) -> {
            producerProps.put(k, v);
        });
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("acks", "all");

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
                String timestamp = sdf.format(new Date(recordMetadata.timestamp()));
                offset[i] = offsetToRead;
                LOG.info("Produced " + payload[i] + " dated " + timestamp + " at partition " + i + " offset " + offset[i]);
            }
            producer.close();

        } catch (Exception e) {

            LOG.error("Failed to produce events to Kafka Cluster");
            LOG.error(e.getMessage());
            e.printStackTrace();
            healthy = false;  // Set cluster health to false if production fails
        }

        Properties consumerProps = new Properties();
        adminProps.forEach((k,v) -> {
            consumerProps.put(k, v);
        });
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");

        int successCount = 0;
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
                        String timestamp = sdf.format(new Date(record.timestamp()));
                        LOG.info("Consumed " + record.value() + " dated " + timestamp + " from partition " + record.partition() + " offset " + offset[i]);
                        if (payload[i].equals(record.value())) {
                            successCount += 1;
                        }
                        if (numberOfMessagesReadSoFar >= numberOfMessagesToRead){
                            keepOnReading = false;
                            break;
                        }
                    }
                }
            }

            healthy = (successCount == partitionCount) ? true : false;
            consumer.close();

        } catch (Exception e) {

            LOG.error("Failed to consume events from Kafka Cluster");
            LOG.error(e.getMessage());
            e.printStackTrace();
            healthy = false;  // Set cluster health to false if consumption fails
        }

        if (healthy)
            LOG.info("Cluster is HEALTHY");
        else
            LOG.info("Cluster is NOT HEALTHY");
    }

    public static void main(String[] args) throws Exception {

        final Properties props = Main.loadProperties("configuration/admin.properties");
        cycleInterval = Long.parseLong(props.getProperty("cycle.interval"));
        maxCycle = Integer.parseInt(props.getProperty("max.cycle"));
        infiniteRun = Boolean.valueOf(props.getProperty("infinite.run"));

        int counter = 1;
        while (counter <= maxCycle || infiniteRun) {
            LOG.info("Run cycle # " + counter);
            run(props);
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
        List<Integer> nodeId =  nodes.stream().map(Node::id).collect(Collectors.toList());

        final Properties props = Main.loadProperties("configuration/broker.properties");
        for (String key : props.stringPropertyNames()) {
            if (nodeId.contains(Integer.valueOf(key))) {
                LOG.info(key + " | " + props.getProperty(key) + " | ACTIVE");
            } else {
                LOG.info(key + " | " + props.getProperty(key) + " | INACTIVE");
                healthy = false;  // Set cluster health to false if any broker is inactive
            }
        }
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

    static String getTopicConfig(AdminClient ac, String topic, String property) throws InterruptedException, ExecutionException {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        Map<ConfigResource, Config> configMap = ac.describeConfigs(Collections.singleton(resource)).all().get();
        final Config config = configMap.get(resource);
        ConfigEntry configEntry = config.get(property);
        return configEntry.value();
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
}

