package redis.core;

public class MortalValue {
    private final String value;
    private long expirationTime = Long.MAX_VALUE;

    public MortalValue(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    public boolean isExpired() {
        return System.currentTimeMillis() > expirationTime;
    }

}
