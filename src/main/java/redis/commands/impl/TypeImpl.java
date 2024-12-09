package redis.commands.impl;


import redis.commands.RedisCommandHandler;
import redis.core.MortalValue;
import redis.core.StorageManager;
import redis.protocol.RESP.RESPEncoder;
import redis.server.ClientHandler;

public class TypeImpl implements RedisCommandHandler {
    private final StorageManager storageManager;

    public TypeImpl(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    @Override
    public void invoke(String[] args, ClientHandler clientHandler) {
        String response = null, key;
        if (args.length != 2) {
            response = "-ERR wrong number of arguments for 'TYPE'\r\n";
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
            return;
        }

        key = args[1];
        MortalValue value = storageManager.get(key);

        if (value == null) {
            response = RESPEncoder.encodeSimpleString("none");
        } else if (value.getValue() instanceof String) {
            response = RESPEncoder.encodeSimpleString("string");
        }

        clientHandler.getWriter().print(response);
        clientHandler.getWriter().flush();

    }
}