package org.eagle;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.apache.commons.lang3.Validate;

public class ConsumerWrapper<K, V> implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerWrapper.class);

    private  KafkaConsumer<K, V> kafkaConsumer;

    private String topic;

    // the bool value that handles the graceful shutdown
    private boolean running = true;

    // this class tracks all inflight kafka messages and acknowledges the message if desired
    private final Tracker tracker;

    // if the poll is not called before the expiration time, the consumer is considered failed
    private final int maxPollExpiration;

    private long lastPoll;

    //blocking queue of all records that need to be processed, the poll() function
    // gives the record of this queue
    private BlockingQueue<ConsumerRecord<K, V>> records;

    private final OffsetCommitCallback offsetCommitCallback;

    private final List<TopicPartition> assignedPartitionsToConsumer;

    // this is the list of all unapplied acks
    private BlockingQueue<PartitionOffset> unAppliedAcks;

    // the thread for handling acks and consuming records
    private final Thread thread ;

    // this is useful when rebalancing in kafka happens
    private final ConsumerRebalanceListener listener;

    public ConsumerWrapper(Builder<K, V> builder) {
        this.kafkaConsumer = new KafkaConsumer<>(builder.props);
        this.tracker = new Tracker();
        this.records = new LinkedBlockingQueue<>(builder.capacity);
        this.unAppliedAcks = new LinkedBlockingQueue<>();
        this.offsetCommitCallback = builder.offsetCommitCallback;
        this.assignedPartitionsToConsumer = new ArrayList<>();
        this.maxPollExpiration = Integer.parseInt(builder.props.getProperty("max.poll.interval.ms"));
        this.listener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                logger.error("onPartitionsRevoked {} Rebalancing happened", partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                logger.error("onPartitionsAssigned {}", partitions);
                assignedPartitionsToConsumer.clear();
                assignedPartitionsToConsumer.addAll(partitions);
                logger.info("Kafka consumer partitions assigned: {}", partitions);
                tracker.reset();
            }
        };
        this.thread = initConsumerThread();
    }


    public void subscribe(String topic) {
        kafkaConsumer.subscribe(Collections.singletonList(topic),listener);
        this.topic = topic;
        thread.setName("KafkaConsumer-" + topic);
    }

    public void start()  {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try{
            executor.submit(() -> {
                Map<String, List<PartitionInfo>> Info =  kafkaConsumer.listTopics();
                if (!Info.containsKey(this.topic)){
                    throw new AssertionError("the topic does not exist");
                }
            }).get(10, TimeUnit.SECONDS);
        } catch (ExecutionException | TimeoutException | InterruptedException e){
            // if list topic has problem the thread blocks
            kafkaConsumer.wakeup();
            throw new AssertionError("the topic does not exist");
        }
        finally {
            executor.shutdown();
        }

        thread.start();
    }

    public ConsumerRecord<K, V> poll() {
        return records.poll();
    }

    public void ack(PartitionOffset message) {
        unAppliedAcks.add(message);
    }

    private void handleUnackedMessages(){
        PartitionOffset message;
        Map<TopicPartition, OffsetAndMetadata> batchOffsets = new HashMap<>();
        while ((message = unAppliedAcks.poll()) != null) {
            Optional<Long> commit = tracker.acknowledge(message.getOffset(),message.getPartition());
            if (commit.isPresent()){
                batchOffsets.put(new TopicPartition(topic,message.getPartition()), new OffsetAndMetadata(message.getOffset()));
            }
        }
        kafkaConsumer.commitAsync(
                batchOffsets,
                offsetCommitCallback
        );
    }

    private void keepConnectionAlive(){
        if (System.currentTimeMillis() - lastPoll < (int) (0.8 * maxPollExpiration)) {
            return;
        }
        try{
            kafkaConsumer.pause(assignedPartitionsToConsumer);
            kafkaConsumer.poll(Duration.ofMillis(0));
            lastPoll = System.currentTimeMillis();
            kafkaConsumer.resume(assignedPartitionsToConsumer);

        }
        catch (IllegalStateException e){
            logger.error(e.getMessage());
        }
    }

    private Thread initConsumerThread(){
        return new Thread(() -> {
            logger.info("started consuming from topic: {}", topic);
            while(running){
                //handle acknowledging
                handleUnackedMessages();
                // put messages into queue
                ConsumerRecords<K,V> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(maxPollExpiration));
                lastPoll = System.currentTimeMillis();
                consumerRecords.forEach(record -> {
                    tracker.track(record.offset(),record.partition());
                    while(!records.offer(record)){
                        //handle acknowledging
                        keepConnectionAlive();
                        handleUnackedMessages();
                        try {
                            keepConnectionAlive();
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
            }
        });
    }

    @Override
    public void close() {
        running = false;
        kafkaConsumer.wakeup();
        thread.interrupt();
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new AssertionError("Unexpected interrupt.", e);
        }
        kafkaConsumer.close();
    }

    public static class Builder<K, V> {
        private Properties props;
        private OffsetCommitCallback offsetCommitCallback;
        private int capacity = 1000; // default capacity

        public Builder<K, V> withProperties(Properties props) {
            this.props = props;
            return this;
        }

        public Builder<K, V> withOffsetCommitCallback(OffsetCommitCallback callback) {
            this.offsetCommitCallback = callback;
            return this;
        }

        public Builder<K, V> withCapacity(int capacity) {
            Validate.isTrue(capacity > 50, "capacity must be greater than 50");
            this.capacity = capacity;
            return this;
        }


        public ConsumerWrapper<K, V> build() {
            if (offsetCommitCallback == null){
                offsetCommitCallback = (o,e)-> {
                   if (e != null){
                       logger.error("the consumer offset cant not be set", e);
                   }
                };
            }
            return new ConsumerWrapper<>(this);
        }
    }

}