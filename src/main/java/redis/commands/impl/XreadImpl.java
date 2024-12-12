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
        if (args.length != 4) {
            illegalNumOfArg(clientHandler);
            return;
        } else if (!args[1].equalsIgnoreCase("streams")) {
            wrongReadHeader(clientHandler);
            return;
        }

        String key = args[2], id = args[3], targetId = null;
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
        List<Object> result = new ArrayList<>(), streamList = new ArrayList<>(), entryList = new ArrayList<>();
        entryList.add(targetId);
        entryList.add(targetEntry.getField().entrySet().stream()
                              .flatMap(e -> Stream.of(e.getKey(), e.getValue()))
                              .collect(Collectors.toList())
        );
        streamList.add(key);
        streamList.add(List.of(entryList));
        result.add(streamList);

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
