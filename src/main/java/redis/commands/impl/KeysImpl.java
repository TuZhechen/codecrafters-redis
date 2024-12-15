package redis.commands.impl;

import redis.commands.RedisCommandHandler;
import redis.commands.TransactionHelper;
import redis.core.StorageManager;
import redis.protocol.RESP.RESPEncoder;
import redis.server.ClientHandler;

public class KeysImpl implements RedisCommandHandler {
    private final StorageManager storageManager;

    public KeysImpl(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    @Override
    public void invoke(String[] args, ClientHandler clientHandler) {
        String response;
        if (args.length < 2) {
            response = "-ERR KEYS requires at least one pattern argument";
            clientHandler.getWriter().print(response);
            return;
        }

        if (TransactionHelper.isHandlingTransaction(clientHandler, args)) return;

        String pattern = args[1];
        String regex = pattern.replace("*", ".*");
        String[] matchedKeys = storageManager.keySet().stream()
                .filter(key -> key.matches(regex))
                .toArray(String[]::new);
        response = RESPEncoder.encodeArray(
                matchedKeys
        );
        clientHandler.getWriter().print(response);
        clientHandler.getWriter().flush();
    }
}
