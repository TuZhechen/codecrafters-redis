package redis.commands.impl;

import redis.commands.RedisCommandHandler;
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
    public void invoke(String[] args, ClientHandler clientHandler) {
        int argLength = args.length, offset = 0;
        boolean isBlockRead = false;
        long blockTime = 0;
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
            illegalNumOfArg(clientHandler);
            return;
        } else if (!args[offset + 1].equalsIgnoreCase("streams")) {
            wrongReadHeader(clientHandler);
            return;
        }

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
                    successResponse(clientHandler, result);
                    return;
                } else {
                    noEntryRetrieved(clientHandler);
                    return;
                }
            }

            if (isBlockRead) {
                if (blockTime == 0) continue;
                boolean isTimeOut = System.currentTimeMillis() - startTime > blockTime;
                if (isTimeOut) {
                    noEntryRetrieved(clientHandler);
                    return;
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

    private void illegalNumOfArg(ClientHandler clientHandler) {
        String response;
        response = "-ERR wrong number of arguments for 'XREAD'\r\n";
        clientHandler.getWriter().print(response);
        clientHandler.getWriter().flush();
    }

    private void wrongReadHeader(ClientHandler clientHandler) {
        String response;
        response = "-ERR 2nd argument should be streams for 'XREAD'\r\n";
        clientHandler.getWriter().print(response);
        clientHandler.getWriter().flush();
    }

    private static void streamNotExist(ClientHandler clientHandler, String key) {
        String response;
        response = "-ERR stream " + key + " does not exist\r\n";
        clientHandler.getWriter().print(response);
        clientHandler.getWriter().flush();
    }

    private void noEntryRetrieved(ClientHandler clientHandler) {
        String response;
        response = RESPEncoder.encodeBulkString("");
        clientHandler.getWriter().print(response);
        clientHandler.getWriter().flush();
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

    private static void successResponse(ClientHandler clientHandler, List<Object> result) {
        String response = RESPEncoder.encodeArray(result.toArray());
        clientHandler.getWriter().print(response);
        clientHandler.getWriter().flush();
    }
}
