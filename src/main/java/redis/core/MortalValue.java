package redis.core;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class MortalValue<T> {
    private final T value;
    private final String type;
    private long expirationTime = -1;

    private String checkType(T value) {
        if (value == null) return "none";
        if (value instanceof String) return "string";
        if (value instanceof List) return "list";
        if (value instanceof Map) return "hash";
        if (value instanceof Set) return "set";
        if (value instanceof RedisStream) return "stream";
        // Add more type mappings as needed
        return "unknown";
    }
    public MortalValue(T value) {
        this.value = value;
        this.type = checkType(value);
    }

    public T getValue() {
        return value;
    }

    public String getType() { return  type; }

    public long getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    public boolean isExpired() {
        return expirationTime > 0 && System.currentTimeMillis() > expirationTime;
    }

}
