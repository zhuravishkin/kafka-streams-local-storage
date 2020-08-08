package com.zhuravishkin.kafkastreamslocalstorage.topology;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zhuravishkin.kafkastreamslocalstorage.model.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KafkaStreamsTopology {
    private final ObjectMapper objectMapper;
    private final Serde<User> userSerde;

    public KafkaStreamsTopology(ObjectMapper objectMapper, Serde<User> userSerde) {
        this.objectMapper = objectMapper;
        this.userSerde = userSerde;
    }

    public Topology kStream(StreamsBuilder kStreamBuilder) {
        kStreamBuilder
                .stream("src-topic", Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(this::getUserFromString)
                .to("out-topic", Produced.with(Serdes.String(), userSerde));
        return kStreamBuilder.build();
    }

    User getUserFromString(String userString) {
        User user = null;
        try {
            user = objectMapper.readValue(userString, User.class);
        } catch (JsonProcessingException e) {
            log.error(e.getMessage(), e);
        }
        return user;
    }
}
