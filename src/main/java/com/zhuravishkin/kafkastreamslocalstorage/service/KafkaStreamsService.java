package com.zhuravishkin.kafkastreamslocalstorage.service;

import com.zhuravishkin.kafkastreamslocalstorage.topology.KafkaStreamsTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Properties;

@Service
public class KafkaStreamsService {
    private final KafkaStreamsConfiguration kafkaStreamsConfiguration;
    private final StreamsBuilder streamsBuilder;
    private final KafkaStreamsTopology kafkaStreamsTopology;

    public KafkaStreamsService(KafkaStreamsConfiguration kafkaStreamsConfiguration, StreamsBuilder streamsBuilder,
                               KafkaStreamsTopology kafkaStreamsTopology) {
        this.kafkaStreamsConfiguration = kafkaStreamsConfiguration;
        this.streamsBuilder = streamsBuilder;
        this.kafkaStreamsTopology = kafkaStreamsTopology;
    }

    @PostConstruct
    public void postConstructor() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
        streamsStart(properties);

    }

    public void streamsStart(Properties properties) {
        kafkaStreamsTopology.kStream(streamsBuilder);
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(properties),
                kafkaStreamsConfiguration.asProperties());
        kafkaStreams.start();
    }
}
