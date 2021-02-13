package com.zhuravishkin.kafkastreamslocalstorage.topology;

import com.zhuravishkin.kafkastreamslocalstorage.model.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@ExtendWith(MockitoExtension.class)
@SpringBootTest
class KafkaStreamsTopologyTest {
    @SpyBean
    private KafkaStreamsConfiguration kStreamsConfigs;

    @SpyBean
    private KafkaStreamsTopology kafkaStreamsTopology;

    @SpyBean
    private StreamsBuilder streamsBuilder;

    @SpyBean
    private Serde<User> userSerde;

    private String inputString;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;
    private TopologyTestDriver topologyTestDriver;

    {
        try {
            inputString = Files.readString(Paths.get("src/test/resources/User.json"));
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    @BeforeEach
    void setUp() {
        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore("test");
        StoreBuilder<KeyValueStore<String, User>> storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), userSerde);
        String inputTopicName = UUID.randomUUID().toString();
        String outputTopicName = UUID.randomUUID().toString();
        topologyTestDriver = new TopologyTestDriver(
                kafkaStreamsTopology.kStream(streamsBuilder, storeBuilder, inputTopicName, outputTopicName),
                kStreamsConfigs.asProperties()
        );
        inputTopic = topologyTestDriver.createInputTopic(
                inputTopicName,
                Serdes.String().serializer(),
                Serdes.String().serializer()
        );
        outputTopic = topologyTestDriver.createOutputTopic(
                outputTopicName,
                Serdes.String().deserializer(),
                Serdes.String().deserializer()
        );
    }

    @AfterEach
    void tearDown() {
        topologyTestDriver.close();
    }

    @Test
    void topologyTest() {
        inputTopic.pipeInput(inputString);
        log.warn(outputTopic.readValue());
        assertTrue(outputTopic.isEmpty());
    }
}
