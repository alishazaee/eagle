package org.eagle;


import org.eagle.configs.Config;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class App {
    public static void main(String[] args) {
        Config config = loadConfig (Path.of("src/main/resources/app.yaml"));
        KafkaBasedConfigureDomains kafkaBasedConfigureDomains = new KafkaBasedConfigureDomains(DomainProto.quality_config.parser(),config);
        kafkaBasedConfigureDomains.start();
        Worker worker = new Worker(config);
        Watcher watcher = new Watcher(config.getRedis(),worker::addDomain);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.schedule(watcher::getConfigs, 5, TimeUnit.MINUTES);

    }

    public static Config loadConfig(Path yamlPath) {
        Config config;
        if (!yamlPath.toFile().exists()) {
            yamlPath = Path.of("src/main/resources/app.yaml");
        }

        try (InputStream stream = Files.newInputStream(yamlPath)) {
            config = new Yaml().loadAs(stream, Config.class);
        } catch (IOException e) {
            throw new AssertionError("Unable to find config file");
        }
        // TODO add validator
        return config;
    }
}
