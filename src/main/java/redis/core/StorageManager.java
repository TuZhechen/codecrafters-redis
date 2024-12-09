package redis.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class StorageManager {
    private final Map<String, MortalValue<?>> store = new ConcurrentHashMap<>();

    public synchronized void put(String key, MortalValue<?> value) {
        store.put(key, value);
    }

    public synchronized <T> MortalValue<T> get(String key, Class<T> type) {
        MortalValue<?> mortalValue = store.get(key);
        if (mortalValue == null || mortalValue.isExpired()) {
            return null;
        }
        if (type.isInstance(mortalValue.getValue())) {
            return (MortalValue<T>) mortalValue;
        }
        throw new ClassCastException("Value not of expected type");
    }

    public synchronized MortalValue<?> getRawValue(String key) {
        MortalValue<?> mortalValue = store.get(key);
        if (mortalValue == null || mortalValue.isExpired()) {
            return null;
        }
        return mortalValue;
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

    public synchronized Map<String, MortalValue<?>> getStoreCopy() {
        return new HashMap<>(store);  // Return a copy of the store
    }
}

