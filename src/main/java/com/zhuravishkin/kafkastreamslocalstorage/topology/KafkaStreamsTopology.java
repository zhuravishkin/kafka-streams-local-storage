package com.zhuravishkin.kafkastreamslocalstorage.topology;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zhuravishkin.kafkastreamslocalstorage.model.User;
import com.zhuravishkin.kafkastreamslocalstorage.service.KafkaStreamsTransformer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaStreamsTopology {
    private final ObjectMapper objectMapper;
    private final Serde<User> userSerde;
    private final KeyValueMapper<String, User, String> numberAsKey = (key, value) -> value.getPhoneNumber();
    private static final String STATE_STORE_NAME = "stateStore";

    public KafkaStreamsTopology(ObjectMapper objectMapper, Serde<User> userSerde) {
        this.objectMapper = objectMapper;
        this.userSerde = userSerde;
    }

    public Topology kStream(StreamsBuilder kStreamBuilder,
                            StoreBuilder<KeyValueStore<String, User>> storeBuilder,
                            String inputTopicName,
                            String outputTopicName,
                            String throughTopicName) {
        kStreamBuilder.addStateStore(storeBuilder);
        kStreamBuilder
                .stream(inputTopicName, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(this::getUserFromString)
                .selectKey(numberAsKey)
                .through(throughTopicName, Produced.with(Serdes.String(), userSerde))
                .transformValues(() -> new KafkaStreamsTransformer(STATE_STORE_NAME), STATE_STORE_NAME)
                .to(outputTopicName, Produced.with(Serdes.String(), userSerde));
        return kStreamBuilder.build();
    }

    public User getUserFromString(String userString) {
        User user = null;
        try {
            user = objectMapper.readValue(userString, User.class);
        } catch (JsonProcessingException e) {
            log.error(e.getMessage(), e);
        }
        log.info("the message is processed");
        return user;
    }
}
