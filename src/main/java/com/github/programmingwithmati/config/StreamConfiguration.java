package com.github.programmingwithmati.config;

import com.github.programmingwithmati.topology.BankBalanceTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.HostInfo;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

import java.util.Properties;

@Slf4j
@Configuration
public class StreamConfiguration {

    @Value("${host.info}")
    private String hostInfo;
    @Value("${kafka.streams.state.dir:/tmp/kafka-streams}")
    private String kafkaStreamsStateDir;


    @Bean
    public Properties kafkaStreamsConfiguration() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-queries");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        properties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, hostInfo);
        properties.put(StreamsConfig.STATE_DIR_CONFIG, kafkaStreamsStateDir);
        return properties;
    }

    @Bean
    public KafkaStreams kafkaStreams(@Qualifier("kafkaStreamsConfiguration") Properties streamConfiguration) {
        var topology = BankBalanceTopology.buildTopology();
        var kafkaStreams = new KafkaStreams(topology, streamConfiguration);

        // clean state directory to start fresh
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        return kafkaStreams;
    }

    @Bean
    public HostInfo hostInfo() {
        log.info("Creating host info: {}", hostInfo);
        var split = hostInfo.split(":");
        return new HostInfo(split[0], Integer.parseInt(split[1]));
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }
}
