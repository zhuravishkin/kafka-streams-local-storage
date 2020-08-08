package com.zhuravishkin.kafkastreamslocalstorage.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ExtendWith(MockitoExtension.class)
@Slf4j
class KafkaStreamsTopologyTest {
    @Autowired
    private KafkaStreamsConfiguration kafkaStreamsConfiguration;
    @SpyBean
    private KafkaStreamsTopology kafkaStreamsTopology;
    @Autowired
    private StreamsBuilder streamsBuilder;
    private String inputString;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;

    {
        try {
            inputString = Files.readString(Paths.get("src/test/resources/User.json"));
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    @BeforeEach
    void setUp() {
        String inputTopicName = UUID.randomUUID().toString();
        String outputTopicName = UUID.randomUUID().toString();
        TopologyTestDriver topologyTestDriver = new TopologyTestDriver(
                kafkaStreamsTopology.kStream(streamsBuilder, inputTopicName, outputTopicName),
                kafkaStreamsConfiguration.asProperties()
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

    @Test
    void kStream() {
        inputTopic.pipeInput(inputString);
        log.warn(outputTopic.readValue());
        assertTrue(outputTopic.isEmpty());
    }
}