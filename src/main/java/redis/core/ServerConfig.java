package redis.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class ServerConfig {
    private final Map<String, String> config = new ConcurrentHashMap<>();

    public void parseArgs(String[] args) {
        for (int i = 0; i < args.length - 1; i++) {
            String key = args[i];
            String value = args[i + 1];
            switch (key) {
                case "--dir":
                case "--dbfilename":
                case "--port":
                    config.put(key.substring(2), value);
                    break;
                case "--replicaof":
                    config.put(key.substring(2), value);
                    config.put("role", "slave");
                    break;
            }
        }
        config.putIfAbsent("role", "master");
    }

    public String get(String key) {
        return config.get(key);
    }

    public boolean containsKey(String key) {
        return config.containsKey(key);
    }

    public boolean isMaster() {
        return !"slave".equalsIgnoreCase(config.get("role"));
    }

    public String getMasterHost() {
        return config.get("replicaof").split(" ")[0];
    }

    public int getMasterPort() {
        return Integer.parseInt(config.get("replicaof").split(" ")[1]);
    }

    public int getPort() {
        return Integer.parseInt(config.getOrDefault("port", "6379"));
    }

    public String getDir() {
        return config.get("dir");
    }

    public String getDbFileName() {
        return config.get("dbfilename");
    }
}

