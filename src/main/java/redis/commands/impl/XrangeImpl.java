package redis.commands.impl;

import redis.commands.RedisCommandHandler;
import redis.core.MortalValue;
import redis.core.RedisStream;
import redis.core.StorageManager;
import redis.protocol.RESP.RESPEncoder;
import redis.server.ClientHandler;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class XrangeImpl implements RedisCommandHandler {
    private final StorageManager storageManager;

    public XrangeImpl(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    @Override
    public void invoke(String[] args, ClientHandler clientHandler) {
        String response;
        if (args.length != 4) {
            illegalNumOfArg(clientHandler);
            return;
        }

        String streamKey = args[1], startId = args[2], endId = args[3];
        MortalValue<RedisStream> v = storageManager.get(streamKey, RedisStream.class);
        if (v == null) {
            streamNotExist(clientHandler,streamKey);
            return;
        }
        RedisStream stream = v.getValue();
        List<List<Object>> streamEntries = stream.getEntries().stream()
                .filter(entry ->
                        XaddImpl.compareStreamIds(entry.getId(), startId) >= 0 &&
                        XaddImpl.compareStreamIds(entry.getId(), endId) <= 0)
                .map(entry -> Arrays.asList(
                        entry.getId(),
                        entry.getField().entrySet().stream()
                                .flatMap(e -> Stream.of(e.getKey(), e.getValue()))
                                .collect(Collectors.toList())
                )).toList();

        response = RESPEncoder.encodeArray(streamEntries.toArray());
        clientHandler.getWriter().print(response);
        clientHandler.getWriter().flush();
    }

    private void illegalNumOfArg(ClientHandler clientHandler) {
        String response;
        response = "-ERR wrong number of arguments for 'XRANGE'\r\n";
        clientHandler.getWriter().print(response);
        clientHandler.getWriter().flush();
    }

    private static void streamNotExist(ClientHandler clientHandler, String key) {
        String response;
        response = "-ERR stream " + key + " does not exist\r\n";
        clientHandler.getWriter().print(response);
        clientHandler.getWriter().flush();
    }
}