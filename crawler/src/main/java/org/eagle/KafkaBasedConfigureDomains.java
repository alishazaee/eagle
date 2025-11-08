package org.eagle;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class KafkaBasedConfigureDomains {
    private static final Logger logger = LoggerFactory.getLogger(KafkaBasedConfigureDomains.class);
    private volatile boolean running = true;
    private final Map<DomainProto.quality_config, ConsumerRecord<?, ?>> unackedRecords = new HashMap<>();

    public KafkaBasedConfigureDomains() {

    }

}
