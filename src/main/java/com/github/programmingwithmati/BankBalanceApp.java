package com.github.programmingwithmati;

import com.github.programmingwithmati.config.StreamConfiguration;
import com.github.programmingwithmati.topology.BankBalanceTopology;
import org.apache.kafka.streams.KafkaStreams;

public class BankBalanceApp {

    public static void main(String[] args) {
        var configuration = StreamConfiguration.getConfiguration();
        var topology = BankBalanceTopology.buildTopology();
        var kafkaStreams = new KafkaStreams(topology, configuration);

        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
