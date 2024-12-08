package redis.commands.impl;

import redis.commands.RedisCommandHandler;
import redis.core.MortalValue;
import redis.core.StorageManager;
import redis.protocol.RESP.RESPEncoder;
import redis.server.ClientHandler;

public class GetImpl implements RedisCommandHandler {
    private final StorageManager storageManager;

    public GetImpl(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    @Override
    public void invoke(String[] args, ClientHandler clientHandler) {
        String response;
        if (args.length < 2) {
            response = "-ERR wrong number of arguments for 'GET'\r\n";
            clientHandler.getWriter().print(response);
            return;
        }
        String key = args[1];
        MortalValue mortalValue = storageManager.get(key);

        if (mortalValue != null && !mortalValue.isExpired()) {
            response = RESPEncoder.encodeBulkString(mortalValue.getValue());
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
        } else {
            System.out.println("Found nothing");
            clientHandler.getWriter().print("$-1\r\n");
            clientHandler.getWriter().flush();
        }
    }
}
