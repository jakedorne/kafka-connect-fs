package com.github.mmolimar.kafka.connect.fs;

import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.Map;

public class InMemoryFsContext implements SourceTaskContext {

    public OffsetStorageReader reader;
    public FsSourceTaskConfig configs;

    public InMemoryFsContext() {
        reader = new InMemoryFsOffsetStorageReader();
    }

    @Override
    public Map<String, String> configs() {
        System.out.println("TURNS OUT YOU PROBABLY NEED TO IMPLEMENT THIS.");
        return null;
    }

    @Override
    public OffsetStorageReader offsetStorageReader() {
        return reader;
    }
}
