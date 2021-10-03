package com.github.programmingwithmati.repository;

import com.github.programmingwithmati.repository.exceptions.ObjectNotFoundException;
import lombok.AllArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.web.client.RestTemplate;

import java.util.Optional;

@AllArgsConstructor
public abstract class GenericKafkaStreamsRepository<K,V> {

    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    protected final HostInfo hostInfo;
    private final KafkaStreams kafkaStreams;
    private final String storeName;
    protected String findRemotelyUri;
    protected RestTemplate restTemplate;

    public V find(K key) {
        var metadata = kafkaStreams.queryMetadataForKey(storeName, key, keySerde.serializer());
        var activeHost = metadata.activeHost();
        if (hostInfo.equals(activeHost)) return findLocally(key);
        return findRemotely(key, activeHost);
    }

    private V findLocally(K key) {
        return Optional
                .ofNullable(getStore().get(key))
                .orElseThrow(() -> new ObjectNotFoundException(key, storeName));

    }

    private ReadOnlyKeyValueStore<K, V> getStore() {
        return kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(
                        storeName,
                        QueryableStoreTypes.keyValueStore()));
    }


    protected abstract V findRemotely(K key, HostInfo hostInfo);
}
