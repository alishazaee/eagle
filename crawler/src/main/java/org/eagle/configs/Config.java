package org.eagle.configs;

public class Config {
    private Kafka kafka;
    private Redis redis;
    private int workerThreadCount;
    private String topic;

    public Config(Redis redis, Kafka kafka, int workerThreadCount, String topic) {
        this.kafka = kafka;
        this.redis= redis;
        this.workerThreadCount = workerThreadCount;
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getWorkerThreadCount() {
        return workerThreadCount;
    }

    public void setWorkerThreadCount(int workerThreadCount) {
        this.workerThreadCount = workerThreadCount;
    }

    public Kafka getKafka() {
        return kafka;
    }

    public void setKafka(Kafka kafka) {
        this.kafka = kafka;
    }

    public Redis getRedis() {
        return redis;
    }

    public void setRedis(Redis redis) {
        this.redis = redis;
    }
}
