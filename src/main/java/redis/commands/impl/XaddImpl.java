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
        if (!isValidStreamId(id)) {
            response = "-ERR make sure stream ID: <milliSecondTimes>-<sequenceNumber>\r\n";
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
            return;
        }

        if (compareStreamIds(id, "0-0") <= 0) {
            response = "-ERR The ID specified in XADD must be greater than 0-0\r\n";
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
            return;
        }

        MortalValue<RedisStream> streamValue = storageManager.get(key, RedisStream.class);
        RedisStream stream;

        if (streamValue == null) {
            stream = new RedisStream();
            storageManager.put(key, new MortalValue<>(stream));
        } else {
            stream = streamValue.getValue();
        }

        if (!stream.getEntries().isEmpty()) {
            String lastId = stream.getEntries()
                                  .get(stream.getEntries().size() - 1)
                                  .getId();
            if (compareStreamIds(id, lastId) <= 0) {
                response = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
                clientHandler.getWriter().print(response);
                clientHandler.getWriter().flush();
                return;
            }
        }

        Map<String, String> entry = new HashMap<>();
        for (int i = 3; i < args.length; i+=2) {
            entry.put(args[i], args[i+1]);
        }
        stream.addEntry(id, entry);

        response = RESPEncoder.encodeBulkString(id);
        clientHandler.getWriter().print(response);
        clientHandler.getWriter().flush();
    }

    private boolean isValidStreamId(String id) {
        String[] idParts = id.split("-");
        if (idParts.length != 2) return false;
        try {
            Long.parseLong(idParts[0]);
            Long.parseLong(idParts[1]);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private int compareStreamIds(String id1, String id2) {
        String[] parts1 = id1.split("-"), parts2 = id2.split("-");
        long timestamp1 = Long.parseLong(parts1[0]), timestamp2 = Long.parseLong(parts2[0]);
        if (timestamp1 != timestamp2) {
            return Long.compare(timestamp1, timestamp2);
        }
        long sequence1 = Long.parseLong(parts1[1]), sequence2 = Long.parseLong(parts2[1]);
        return Long.compare(sequence1, sequence2);
    }
}
