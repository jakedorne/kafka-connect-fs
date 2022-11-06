package com.github.mmolimar.kafka.connect.fs;

import org.apache.kafka.connect.source.SourceRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InMemoryFsSourceTask extends FsSourceTask {

    private List<SourceRecord> batchRecords = new ArrayList<>();

    @Override
    public void commit() throws InterruptedException {
        if (batchRecords == null) return;
        batchRecords.forEach(sourceRecord -> InMemoryOffsetStore.Offsets.put(
                (Map<String, Object>) sourceRecord.sourcePartition() ,
                (Map<String, Object>) sourceRecord.sourceOffset()
        ));
    }

    @Override
    public List<SourceRecord> poll() {
        List<SourceRecord> records = super.poll();
        batchRecords = records;
        try {
            this.commit();
        } catch (InterruptedException exception) {}

        return records;
    }
}
