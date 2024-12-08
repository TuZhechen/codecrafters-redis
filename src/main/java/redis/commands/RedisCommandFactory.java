package redis.commands;

import redis.commands.impl.*;
import redis.core.ServerConfig;
import redis.core.StorageManager;

public class RedisCommandFactory {
    private final StorageManager storageManager;
    private final ServerConfig serverConfig;

    public RedisCommandFactory(StorageManager storageManager, ServerConfig serverConfig) {
        this.storageManager = storageManager;
        this.serverConfig = serverConfig;
    }

    public RedisCommandHandler getCommandHandler(String command) {
        switch (command) {
            case "PING":
                return new PingImpl();
            case "ECHO":
                return new EchoImpl();
            case "SET":
                return new SetImpl(storageManager);
            case "GET":
                return new GetImpl(storageManager);
            case "CONFIG":
                return new ConfigImpl(serverConfig);
            case "KEYS":
                return new KeysImpl(storageManager);
            case "INFO":
                return new InfoImpl(serverConfig);
            case "REPLCONF":
                return new ReplConfImpl();
            case "PSYNC":
                return new PsyncImpl();
            case "WAIT":
                return new WaitImpl();
            default:
                return null;
        }
    }
}
