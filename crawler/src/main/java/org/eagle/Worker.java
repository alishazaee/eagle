package org.eagle;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eagle.configs.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Worker {
    private static final Logger logger = LoggerFactory.getLogger(Worker.class);
    private final Thread[] crawlerThreads;
    private final BlockingQueue<DomainProto.domain> domainConfigs;
    private volatile boolean running = true;
    private final KafkaProducer<byte[],byte[]> producer;
    private final String topic;

    public Worker(Config appConfig) {
        this.crawlerThreads = new Thread[appConfig.getWorkerThreadCount()];
        this.topic = appConfig.getTopic();
        this.domainConfigs = new LinkedBlockingQueue<>();
        this.producer = new KafkaProducer<>(appConfig.getKafka().getProperties());

        for (int i = 0; i < appConfig.getWorkerThreadCount(); i++) {
            crawlerThreads[i] = new Thread(this::startCrawl);
            crawlerThreads[i].setName("crawler-" + i);
        }

    }

    public void addDomain(DomainProto.domain domainConfig) {
        if (!domainConfigs.offer(domainConfig)) {
            close();
            throw new AssertionError("Adding to new domain queue failed!");
        }
    }

    private void startCrawl()  {
        while (running){
            try{
                DomainProto.domain domainConf = domainConfigs.take();
                if(domainConf.getType() == DomainProto.Type.QOE){
                    MetricsProto.DigMetrics dnsMetric = NetQualityStat.digStat(domainConf.getDomainName(),"8.8.8.8");
                    MetricsProto.Metrics metrics = MetricsProto.Metrics.newBuilder().setDigMetrics(dnsMetric).build();
                    ProducerRecord<byte[],byte[]> producerRecord = new ProducerRecord<>(topic,domainConf.getDomainName().getBytes(),metrics.toByteArray());
                    producer.send(producerRecord);
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
