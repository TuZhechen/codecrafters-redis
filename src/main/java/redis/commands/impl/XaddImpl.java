package redis.commands.impl;

import redis.commands.RedisCommandHandler;
import redis.commands.TransactionHelper;
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
    public String invoke(String[] args, ClientHandler clientHandler, boolean invokeFromExec) {
        String response;
        if (args.length < 5 || (args.length - 3) % 2 != 0) {
            response = "ERR wrong number of arguments for 'XADD'";
            return TransactionHelper.errorResponse(clientHandler, response, invokeFromExec);
        }

        if (TransactionHelper.isHandlingTransaction(clientHandler, args)) return null;

        String key = args[1], id = args[2];
        if (!isValidEntryId(id)) {
            return invalidIdResponse(clientHandler, invokeFromExec);
        }

        if (isExplictStreamId(id) && compareStreamIds(id, "0-0") <= 0) {
            return leqZeroIdResponse(clientHandler, invokeFromExec);
        }

        MortalValue<RedisStream> streamValue = storageManager.get(key, RedisStream.class);
        RedisStream stream;

        if (streamValue == null) {
            stream = new RedisStream();
            storageManager.put(key, new MortalValue<>(stream));
        } else {
            stream = streamValue.getValue();
        }

        long currTimeStamp = id.equals("*") ? System.currentTimeMillis() : Long.parseLong(id.split("-")[0]);
        if (!stream.getEntries().isEmpty()) {
            String lastId = stream.getEntries()
                                  .getLast()
                                  .getId();
            long lastTimeStamp = Long.parseLong(lastId.split("-")[0]);
            long lastIdSequence = Long.parseLong(lastId.split("-")[1]);
            if (isExplictStreamId(id)) {
                if (compareStreamIds(id, lastId) <= 0) {
                    return invalidOrderResponse(clientHandler, invokeFromExec);
                }
            } else {
                if (lastTimeStamp > currTimeStamp) {
                    return invalidOrderResponse(clientHandler, invokeFromExec);
                }
                long currIdSequence =  currTimeStamp > lastTimeStamp ? 0 : lastIdSequence + 1;
                id = currTimeStamp + "-" + currIdSequence;
            }
        } else if(!isExplictStreamId(id)){
            long currIdSequence = currTimeStamp > 0 ? 0: 1;
            id = currTimeStamp + "-" + currIdSequence;
        }

        Map<String, String> entry = new HashMap<>();
        for (int i = 3; i < args.length; i+=2) {
            entry.put(args[i], args[i+1]);
        }
        stream.addEntry(id, entry);

        response = RESPEncoder.encodeBulkString(id);
        if (!invokeFromExec) {
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
        }
        return response;
    }

    private String invalidOrderResponse(ClientHandler clientHandler, boolean invokeFromExec) {
        String response = "ERR The ID specified in XADD is equal or smaller than the target stream top item";
        return TransactionHelper.errorResponse(clientHandler, response, invokeFromExec);
    }

    private String leqZeroIdResponse(ClientHandler clientHandler, boolean invokeFromExec) {
        String response = "ERR The ID specified in XADD must be greater than 0-0";
        return TransactionHelper.errorResponse(clientHandler, response, invokeFromExec);
    }

    private String invalidIdResponse(ClientHandler clientHandler, boolean invokeFromExec) {
        String response = "ERR illegal entry ID";
        return TransactionHelper.errorResponse(clientHandler, response, invokeFromExec);
    }

    private boolean isValidEntryId(String id) {
        if (id.equals("*")) return true;
        String[] idParts = id.split("-");
        if (idParts.length != 2) return false;
        return true;
    }

    private boolean isExplictStreamId(String id) {
        try {
            String[] idParts = id.split("-");
            Long.parseLong(idParts[0]);
            Long.parseLong(idParts[1]);
        } catch (RuntimeException e) {
            return false;
        }
        return true;
    }

    public static int compareStreamIds(String id1, String id2) {
        if (id2.equals("-")) return 1;
        else if (id2.equals("+")) return -1;

        String[] parts1 = id1.split("-"), parts2 = id2.split("-");
        long timestamp1 = Long.parseLong(parts1[0]), timestamp2 = Long.parseLong(parts2[0]);
        if (timestamp1 != timestamp2) {
            return Long.compare(timestamp1, timestamp2);
        }
        long sequence1 = Long.parseLong(parts1[1]), sequence2 = Long.parseLong(parts2[1]);
        return Long.compare(sequence1, sequence2);
    }
}
