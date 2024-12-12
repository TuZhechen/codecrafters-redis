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
        String response;
        int argLength = args.length;
        if (argLength < 4 && (argLength - 2) % 2 != 0) {
            illegalNumOfArg(clientHandler);
            return;
        } else if (!args[1].equalsIgnoreCase("streams")) {
            wrongReadHeader(clientHandler);
            return;
        }

        int numOfStreams = (argLength - 2) / 2;
        String[] keys = new String[numOfStreams], ids = new String[numOfStreams];

        for (int i = 0; i < numOfStreams; i++) {
            keys[i] = args[i+2];
            ids[i] = args[i+2+numOfStreams];
        }

        List<Object> result = new ArrayList<>();
        for (int i = 0; i < numOfStreams; i++) {
            String key = keys[i] , id = ids[i], targetId = null;
            List<Object> streamList = new ArrayList<>(), entryList = new ArrayList<>();
            MortalValue<RedisStream> v = storageManager.get(key, RedisStream.class);
            if (v == null) {
                streamNotExist(clientHandler, key);
                return;
            }
            RedisStream stream = v.getValue();
            RedisStream.StreamEntry targetEntry = null;
            for (RedisStream.StreamEntry entry: stream.getEntries()) {
                if(XaddImpl.compareStreamIds(entry.getId(), id) > 0 ) {
                    targetId = entry.getId();
                    targetEntry = entry;
                    break;
                }
            }
            if (targetId == null) {
                noEntryRetrieved(clientHandler);
                return;
            }

            entryList.add(targetId);
            entryList.add(targetEntry.getField().entrySet().stream()
                                  .flatMap(e -> Stream.of(e.getKey(), e.getValue()))
                                  .collect(Collectors.toList())
            );
            streamList.add(key);
            streamList.add(List.of(entryList));
            result.add(streamList);
        }


        response = RESPEncoder.encodeArray(result.toArray());
        System.out.println(response);
        clientHandler.getWriter().print(response);
        clientHandler.getWriter().flush();
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
        response = RESPEncoder.encodeSimpleString("No entry retrieved");
        clientHandler.getWriter().print(response);
        clientHandler.getWriter().flush();
    }
}
