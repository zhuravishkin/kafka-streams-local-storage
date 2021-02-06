package com.zhuravishkin.kafkastreamslocalstorage.config;

import com.zhuravishkin.kafkastreamslocalstorage.model.User;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaStreamsConfig {
    private static final String STATE_STORE_NAME = "stateStore";

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streamsApplicationId");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "streamsGroupId");
        properties.put(StreamsConfig.CLIENT_ID_CONFIG, "streamsClientId");

        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
        properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class.getName());

        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "1");

        return new KafkaStreamsConfiguration(properties);
    }

    @Bean
    public StreamsBuilder streamsBuilder() {
        return new StreamsBuilder();
    }

    @Bean
    public Serde<User> userSerde() {
        return Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(User.class));
    }

    @Bean
    public Map<String, String> changeLogConfigs() {
        Map<String, String> changeLogConfigs = new HashMap<>();
        changeLogConfigs.put("retention.ms", "172800000");
        changeLogConfigs.put("retention.bytes", "100000000");
        changeLogConfigs.put("cleanup.policy", "compact,delete");
        return changeLogConfigs;
    }

    @Bean
    public StoreBuilder<KeyValueStore<String, User>> storeBuilder() {
        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(STATE_STORE_NAME);
        StoreBuilder<KeyValueStore<String, User>> keyValueStoreStoreBuilder = Stores.keyValueStoreBuilder(
                storeSupplier,
                Serdes.String(),
                userSerde()
        );
        keyValueStoreStoreBuilder.withLoggingEnabled(changeLogConfigs());
        return keyValueStoreStoreBuilder;
    }
}
