package redis.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class StorageManager {
    private final Map<String, MortalValue> store = new ConcurrentHashMap<>();

    public synchronized void put(String key, MortalValue value) {
        store.put(key, value);
    }

    public synchronized MortalValue get(String key) {
        return store.get(key);
    }

    public synchronized boolean containsKey(String key) {
        return store.containsKey(key);
    }

    public synchronized void delete(String key) {
        store.remove(key);
    }

    public synchronized Set<String> keySet() {
        return store.keySet();
    }

    public synchronized Map<String, MortalValue> getStore() {
        return store;
    }

    public synchronized Map<String, MortalValue> getStoreCopy() {
        return new HashMap<>(store);  // Return a copy of the store
    }
}

