package org.eagle;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import org.eagle.configs.Redis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Properties;
import java.util.function.Consumer;

public class Watcher {

    private final Jedis jedis;
    private final Consumer<DomainProto.domain> consumer;
    private static final Logger logger = LoggerFactory.getLogger(Watcher.class);

    public Watcher(Redis redisConf, Consumer<DomainProto.domain> consumer) {
        jedis = new Jedis(redisConf.getHost(),redisConf.getPort());
        this.consumer = consumer;
    }

    public void getConfigs() {
        byte[] storedBytes = jedis.get("config".getBytes());
        if (storedBytes != null) {
            try {
                DomainProto.quality_config storedConfig = DomainProto.quality_config.parseFrom(storedBytes);
                for(DomainProto.domain domain: storedConfig.getConfigsList()){
                    consumer.accept(domain);
                }
            }
            catch (InvalidProtocolBufferException e) {
                logger.error(e.getMessage());
            }

        }

    }
}
