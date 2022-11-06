package com.github.mmolimar.kafka.connect.fs;

import java.util.HashMap;
import java.util.Map;

public class InMemoryOffsetStore {
    public static Map<Map<String, Object>, Map<String, Object>> Offsets = new HashMap<>();
}
