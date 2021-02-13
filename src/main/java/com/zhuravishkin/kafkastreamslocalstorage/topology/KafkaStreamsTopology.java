package com.zhuravishkin.kafkastreamslocalstorage.topology;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zhuravishkin.kafkastreamslocalstorage.model.User;
import com.zhuravishkin.kafkastreamslocalstorage.service.KTableValueJoiner;
import com.zhuravishkin.kafkastreamslocalstorage.service.KafkaStreamsTransformer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaStreamsTopology {
    private final ObjectMapper objectMapper;
    private final Serde<User> userSerde;
    private static final String STATE_STORE_NAME = "stateStore";

    public KafkaStreamsTopology(ObjectMapper objectMapper, Serde<User> userSerde) {
        this.objectMapper = objectMapper;
        this.userSerde = userSerde;
    }

    public Topology kStream(StreamsBuilder kStreamBuilder,
                            StoreBuilder<KeyValueStore<String, User>> storeBuilder,
                            String inputTopicName,
                            String outputTopicName) {
        kStreamBuilder.addStateStore(storeBuilder);

        KTable<String, User> kTable = kStreamBuilder
                .table("streamsApplicationId-stateStore-changelog", Consumed.with(Serdes.String(), userSerde), Materialized.as(STATE_STORE_NAME));

        kStreamBuilder
                .stream(inputTopicName, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(this::getUserFromString)
                .repartition(Repartitioned.with(Serdes.String(), userSerde).withName("repartition"))
                .transformValues(() -> new KafkaStreamsTransformer(STATE_STORE_NAME), STATE_STORE_NAME)
                .leftJoin(kTable, new KTableValueJoiner())
                .filter((key, value) -> value != null)
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
