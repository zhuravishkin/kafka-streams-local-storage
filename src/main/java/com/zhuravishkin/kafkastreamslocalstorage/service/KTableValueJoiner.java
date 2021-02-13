package com.zhuravishkin.kafkastreamslocalstorage.service;

import com.zhuravishkin.kafkastreamslocalstorage.model.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueJoiner;

@Slf4j
public class KTableValueJoiner implements ValueJoiner<User, User, User> {
    @Override
    public User apply(User current, User stored) {
        if (stored == null) return null;
        log.info(stored.toString());
        current.setBalance(current.getBalance() + stored.getBalance());
        return current;
    }
}
