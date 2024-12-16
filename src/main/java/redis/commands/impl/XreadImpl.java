package redis.commands.impl;

import redis.commands.RedisCommandHandler;
import redis.commands.TransactionHelper;
import redis.core.MortalValue;
import redis.core.RedisStream;
import redis.core.StorageManager;
import redis.protocol.RESP.RESPEncoder;
import redis.server.ClientHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class XreadImpl implements RedisCommandHandler {
    private final StorageManager storageManager;

    public XreadImpl(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    @Override
    public String invoke(String[] args, ClientHandler clientHandler, boolean invokeFromExec) {
        int argLength = args.length, offset = 0;
        boolean isBlockRead = false;
        long blockTime = 0;
        String errorResponse;
        if (args[1].equalsIgnoreCase("block")) {
            try {
                blockTime = Long.parseLong(args[2]);
            } catch (IndexOutOfBoundsException e) {
                System.err.println("Illegal block read");
            } catch (NumberFormatException e) {
                System.err.println("Invalid block time: " + args[2]);
            }
            offset = 2;
            isBlockRead = blockTime >= 0;
        }
        if (argLength < 4 && (argLength - offset - 2) % 2 != 0) {
            return illegalNumOfArg(clientHandler, invokeFromExec);
        } else if (!args[offset + 1].equalsIgnoreCase("streams")) {
            return wrongReadHeader(clientHandler, invokeFromExec);
        }

        if (TransactionHelper.isHandlingTransaction(clientHandler, args)) return null;

        int numOfStreams = (argLength - offset - 2) / 2;
        String[] keys = new String[numOfStreams], ids = new String[numOfStreams];
        for (int i = 0; i < numOfStreams; i++) {
            keys[i] = args[offset + i + 2];
            String id = args[offset + i + 2 + numOfStreams];
            if (id.equals("$")) {
                RedisStream.StreamEntry lastEntryFromSnapshot = getLastEntryFromSnapshot(keys[i]);
                if (lastEntryFromSnapshot != null) {
                    id = lastEntryFromSnapshot.getId();
                }
            }
            ids[i] = id;
        }

        long startTime = isBlockRead ? System.currentTimeMillis() : 0;
        List<Object> result = new ArrayList<>();
        while (true) {
            result.clear();
            boolean entriesFound = processStreams(keys, ids, result);

            if (!isBlockRead || entriesFound) {
                if (entriesFound) {
                    return successResponse(clientHandler, result, invokeFromExec);
                } else {
                    return noEntryRetrieved(clientHandler, invokeFromExec);
                }
            }

            if (isBlockRead) {
                if (blockTime == 0) continue;
                boolean isTimeOut = System.currentTimeMillis() - startTime > blockTime;
                if (isTimeOut) {
                    return noEntryRetrieved(clientHandler, invokeFromExec);
                }
            }
        }
    }

    private RedisStream.StreamEntry getLastEntryFromSnapshot(String keys) {
        MortalValue<RedisStream> mv = storageManager.get(keys, RedisStream.class);
        if (mv != null) {
            RedisStream stream = mv.getValue();
            List<RedisStream.StreamEntry> entries  = stream.getEntries();
            if (!entries.isEmpty()) {
                return entries.getLast();
            }
        }
        return null;
    }

    private boolean processStreams(String[] keys, String[] ids, List<Object> result) {
        boolean entriesFound = false;
        for (int i = 0; i < keys.length; i++) {
            String key = keys[i] , id = ids[i];
            MortalValue<RedisStream> v = storageManager.get(key, RedisStream.class);
            if (v == null) continue;

            RedisStream stream = v.getValue();
            RedisStream.StreamEntry targetEntry = findTargetEntry(stream, id);
            if (targetEntry != null) {
                addEntry(result, key, targetEntry);
                entriesFound = true;
            }
        }
        return entriesFound;
    }

    private String illegalNumOfArg(ClientHandler clientHandler, boolean invokeFromExec) {
        String response = "ERR wrong number of arguments for 'XREAD'";
        return TransactionHelper.errorResponse(clientHandler, response, invokeFromExec);
    }

    private String wrongReadHeader(ClientHandler clientHandler, boolean invokeFromExec) {
        String response = "ERR 2nd argument should be 'streams' for 'XREAD'";
        return TransactionHelper.errorResponse(clientHandler, response, invokeFromExec);
    }

    private String streamNotExist(ClientHandler clientHandler, boolean invokeFromExec) {
        String response = "ERR stream does not exist";
        return TransactionHelper.errorResponse(clientHandler, response, invokeFromExec);
    }

    private String noEntryRetrieved(ClientHandler clientHandler, boolean invokeFromExec) {
        String response;
        response = RESPEncoder.encodeBulkString("");
        if (!invokeFromExec) {
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
        }
        return response;
    }

    private RedisStream.StreamEntry findTargetEntry(RedisStream stream, String id) {
        for (RedisStream.StreamEntry entry: stream.getEntries()) {
            if(XaddImpl.compareStreamIds(entry.getId(), id) > 0 ) {
                return entry;
            }
        }
        return null;
    }

    private void addEntry(List<Object> result, String key, RedisStream.StreamEntry target) {
        List<Object> entryList = new ArrayList<>();
        entryList.add(target.getId());
        entryList.add(target.getField().entrySet().stream()
                              .flatMap(e -> Stream.of(e.getKey(), e.getValue()))
                              .collect(Collectors.toList())
        );

        List<Object> streamList = new ArrayList<>();
        streamList.add(key);
        streamList.add(List.of(entryList));
        result.add(streamList);
    }

    private static String successResponse(ClientHandler clientHandler, List<Object> result, boolean invokeFromExec) {
        String response = RESPEncoder.encodeArray(result.toArray());
        if (!invokeFromExec) {
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
        }
        return response;
    }
}
