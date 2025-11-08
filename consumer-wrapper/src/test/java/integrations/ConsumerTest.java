package integrations;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.shahin.ConsumerWrapper;
import org.shahin.PartitionOffset;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class ConsumerTest {
    private  KafkaContainer kafkaContainer;
    private String topic;


    private Properties createConsumerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaContainer.getBootstrapServers());
        props.put("group.id", "test-group");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("session.timeout.ms", "6000");
        props.put("heartbeat.interval.ms", "2000");
        props.put("max.poll.interval.ms", "6000");
        return props;
    }

    private Properties createProducerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaContainer.getBootstrapServers());
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        return props;
    }

    @BeforeEach
    public void setup() {
        kafkaContainer = new KafkaContainer(DockerImageName.parse("apache/kafka-native:4.0.0"));
        kafkaContainer.start();
        topic = "test-topic";

    }


    public void sendMessages(int count){
        KafkaProducer<String, String> producer = new KafkaProducer<>(createProducerProps());

        for(int i=0; i<count; i++){
            String key = "Key " + i;
            String value = "Value " + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record);
        }
        producer.flush();

        producer.close();

    }
    @Test
    public void TestConsumeMessages() {
        ConsumerWrapper<String,String> kafkaConsumer = new
                ConsumerWrapper.Builder<String,String>()
                .withProperties(createConsumerProps())
                .withCapacity(51)
                .build();;

        sendMessages(100);

        kafkaConsumer.subscribe(topic);
        kafkaConsumer.start();

        List<ConsumerRecord<String, String>> consumed = new ArrayList<>();

        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .until(() -> {
                    ConsumerRecord<String, String> record;
                    while ((record = kafkaConsumer.poll()) != null) {
                        consumed.add(record);
                        kafkaConsumer.ack(new PartitionOffset(record.offset(), record.partition()));
                    }
                    return consumed.size() == 100;
                });

        Assertions.assertEquals(100, consumed.size());

        kafkaConsumer.close();
    }

    @Test
    public void TestTopicNotExistThrowsException() {
        ConsumerWrapper<String,String> kafkaConsumer = new
                ConsumerWrapper.Builder<String,String>()
                .withProperties(createConsumerProps())
                .withCapacity(51)
                .build();

        kafkaConsumer.subscribe("topic_not_exist");
        AssertionError error = Assertions.assertThrows(AssertionError.class, kafkaConsumer::start);
        Assertions.assertEquals("the topic does not exist", error.getMessage());
    }




}
