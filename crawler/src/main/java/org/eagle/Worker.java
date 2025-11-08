package org.eagle;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Worker {
    private static final Logger logger = LoggerFactory.getLogger(KafkaBasedConfigureDomains.class);
    private final int threadCount;
    private final Thread[] crawlerThreads;
    private final BlockingQueue<DomainProto.quality_config> domainConfigs;
    private volatile boolean running = true;
    private final KafkaProducer<byte[],byte[]> producer;
    private final String topic;

    public Worker(int threadCount, Properties props, String topic) {
        this.threadCount = threadCount;
        this.crawlerThreads = new Thread[threadCount];
        this.topic = topic;
        this.domainConfigs = new LinkedBlockingQueue<>();
        this.producer = new KafkaProducer<>(props);

        for (int i = 0; i < threadCount; i++) {
            crawlerThreads[i] = new Thread(this::startCrawl);
            crawlerThreads[i].setName("crawler-" + i);
        }

    }

    public void addDomain(DomainProto.quality_config domainConfig) {
        if (!domainConfigs.offer(domainConfig)) {
            close();
            throw new AssertionError("Adding to new domain queue failed!");
        }
    }

    private void startCrawl()  {
        while (running){
            try{
                    DomainProto.quality_config qualityConfigs = domainConfigs.take();
                    for(DomainProto.domain domain : qualityConfigs.getConfigsList()){
                        if(domain.getType() == DomainProto.Type.QOE){
                            MetricsProto.DigMetrics dnsMetric = NetQualityStat.digStat(domain.getDomainName(),"8.8.8.8");
                            MetricsProto.Metrics metrics = MetricsProto.Metrics.newBuilder().setDigMetrics(dnsMetric).build();
                            ProducerRecord<byte[],byte[]> producerRecord = new ProducerRecord<>(topic,domain.getDomainName().getBytes(),metrics.toByteArray());
                            producer.send(producerRecord);
                        }
                    }
            }
            catch (InterruptedException e){
                close();
                throw new AssertionError("unable to take from domain queue "+e);
            }

        }
    }

    public void close() {
        running = false;
        logger.error("closing crawler threads ...");
        for (Thread thread : crawlerThreads) {
            thread.interrupt();
        }
        try {
            for (Thread thread : crawlerThreads) {
                thread.join();
            }
        }
        catch (InterruptedException e) {
            throw new AssertionError("Interrupted when waiting ingester threads to terminate", e);
        }
        producer.close();
    }

}
