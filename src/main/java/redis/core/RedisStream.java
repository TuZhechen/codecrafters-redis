package redis.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RedisStream {
    private final List<StreamEntry> entries;

    public RedisStream() {
        entries = new ArrayList<>();
    }

    public static class StreamEntry {
        private final String id;
        private final Map<String, String> fields;

        public StreamEntry(String id, Map<String, String> fields) {
            this.id = id;
            this.fields = new LinkedHashMap<>(fields);
        }
        public String getId() { return id; }

        public Map<String, String> getField() { return new LinkedHashMap<>(fields); }
    }

    public void addEntry(String id, Map<String, String> fields) {
        // Optional: Add validation for ID format and uniqueness
        entries.add(new StreamEntry(id, fields));
    }

    public List<StreamEntry> getEntries() {
        return new ArrayList<>(entries);
    }
}
