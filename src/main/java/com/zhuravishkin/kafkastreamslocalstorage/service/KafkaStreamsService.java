package com.zhuravishkin.kafkastreamslocalstorage.service;

import com.zhuravishkin.kafkastreamslocalstorage.model.User;
import com.zhuravishkin.kafkastreamslocalstorage.topology.KafkaStreamsTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Properties;

@Service
public class KafkaStreamsService {
    private final KafkaStreamsConfiguration kStreamsConfigs;
    private final StreamsBuilder streamsBuilder;
    private final KafkaStreamsTopology kafkaStreamsTopology;
    private final StoreBuilder<KeyValueStore<String, User>> storeBuilder;

    public KafkaStreamsService(KafkaStreamsConfiguration kStreamsConfigs,
                               StreamsBuilder streamsBuilder,
                               KafkaStreamsTopology kafkaStreamsTopology,
                               StoreBuilder<KeyValueStore<String, User>> storeBuilder) {
        this.kStreamsConfigs = kStreamsConfigs;
        this.streamsBuilder = streamsBuilder;
        this.kafkaStreamsTopology = kafkaStreamsTopology;
        this.storeBuilder = storeBuilder;
    }

    @PostConstruct
    public void postConstructor() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
        streamsStart(properties);

    }

    public void streamsStart(Properties properties) {
        kafkaStreamsTopology.kStream(
                streamsBuilder,
                storeBuilder,
                "src-topic",
                "out-topic",
                "streamsApplicationId-stateStore-changelog",
                "kTableStore",
                "repartition"
        );
        KafkaStreams kafkaStreams = new KafkaStreams(
                streamsBuilder.build(properties),
                kStreamsConfigs.asProperties()
        );
        kafkaStreams.start();
    }
}
