package org.eagle;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eagle.configs.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.Parser;
import redis.clients.jedis.Jedis;
import java.util.Properties;

public class KafkaBasedConfigureDomains {
    private static final Logger logger = LoggerFactory.getLogger(KafkaBasedConfigureDomains.class);
    private volatile boolean running = true;
    private final ConsumerWrapper<byte[],byte[]> consumerWrapper;
    private final Parser<DomainProto.quality_config> parser;
    private final Jedis jedis;

    public KafkaBasedConfigureDomains(Parser<DomainProto.quality_config> parser, Config appConfig) {
        consumerWrapper = new ConsumerWrapper.Builder<byte[],byte[]>().withProperties(appConfig.getKafka().getProperties()).withCapacity(100).build();
        consumerWrapper.subscribe(appConfig.getTopic());
        this.parser = parser;
        jedis = new Jedis(appConfig.getRedis().getHost(), appConfig.getRedis().getPort());
    }

    public void start() {
       while (running) {
           ConsumerRecord<byte[],byte[]> record = consumerWrapper.poll();
           try{
               logger.info("new config has been received");
               DomainProto.quality_config config = parser.parseFrom(record.value());
               byte[] protobufBytes = config.toByteArray();
               jedis.set("config".getBytes(), protobufBytes);
           }
           catch (InvalidProtocolBufferException e){
               logger.error(e.toString());
               logger.error("could not parse record");
           }
       }
    }



}
