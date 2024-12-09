package redis.commands.impl;

import redis.commands.RedisCommandHandler;
import redis.core.MortalValue;
import redis.core.RedisStream;
import redis.core.StorageManager;
import redis.protocol.RESP.RESPEncoder;
import redis.server.ClientHandler;

import java.util.HashMap;
import java.util.Map;

public class XaddImpl implements RedisCommandHandler {
    private final StorageManager storageManager;

    public XaddImpl(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    @Override
    public void invoke(String[] args, ClientHandler clientHandler) {
        String response;
        if (args.length < 5 || (args.length - 3) % 2 != 0) {
            response = "-ERR wrong number of arguments for 'XADD'\r\n";
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
            return;
        }
        String key = args[1], id = args[2];
        if(!storageManager.containsKey(key)) {
            storageManager.put(key, new MortalValue<> (new RedisStream()));
        }

        Map<String, String> entry = new HashMap<>();
        for (int i = 3; i < args.length; i+=2) {
            entry.put(args[i], args[i+1]);
        }
        storageManager.get(key, RedisStream.class)
                .getValue().addEntry(id, entry);

        response = RESPEncoder.encodeBulkString(id);
        clientHandler.getWriter().print(response);
        clientHandler.getWriter().flush();
    }
}
