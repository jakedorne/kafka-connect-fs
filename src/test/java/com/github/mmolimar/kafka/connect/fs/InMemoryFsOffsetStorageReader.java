package com.github.mmolimar.kafka.connect.fs;

import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class InMemoryFsOffsetStorageReader implements OffsetStorageReader {
    @Override
    public <T> Map<String, Object> offset(Map<String, T> partition) {
        return InMemoryOffsetStore.Offsets.get(partition);
    }

    @Override
    public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
        Map<Map<String, T>, Map<String, Object>> offsets = new HashMap<>();
        for (Map.Entry<Map<String, Object>, Map<String, Object>> entry : InMemoryOffsetStore.Offsets.entrySet()) {
            if (partitions.contains(entry.getKey())) {

                offsets.put((Map<String, T>) entry.getKey(), entry.getValue());
            }
        }
        return offsets;
    }
}
