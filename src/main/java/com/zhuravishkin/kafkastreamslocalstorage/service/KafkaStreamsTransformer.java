package com.zhuravishkin.kafkastreamslocalstorage.service;

import com.zhuravishkin.kafkastreamslocalstorage.model.User;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class KafkaStreamsTransformer implements ValueTransformer<User, User> {
    private KeyValueStore<String, User> stateStore;
    private final String storeName;
    private ProcessorContext context;

    public KafkaStreamsTransformer(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        stateStore = (KeyValueStore<String, User>) this.context.getStateStore(storeName);
    }

    @Override
    public User transform(User user) {
        User storedUser = stateStore.get(user.getPhoneNumber());
        if (storedUser != null) {
            user.setBalance(user.getBalance() + storedUser.getBalance());
        }
        stateStore.put(user.getPhoneNumber(), user);
        return user;
    }

    @Override
    public void close() {
    }
}
