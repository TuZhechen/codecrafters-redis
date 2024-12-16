package redis.commands.impl;

import redis.commands.RedisCommandHandler;
import redis.commands.TransactionHelper;
import redis.protocol.RESP.RESPEncoder;
import redis.replication.ReplicaManager;
import redis.server.ClientHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class WaitImpl implements RedisCommandHandler {
    private static final AtomicInteger ackedReplicaCount = new AtomicInteger(0);

    @Override
    public String invoke(String[] args, ClientHandler clientHandler, boolean invokeFromExec) {
        String response;
        if (args.length != 3) {
            response = "ERR wrong number of arguments for 'WAIT'";
            return TransactionHelper.errorResponse(clientHandler, response, invokeFromExec);
        }

        if (TransactionHelper.isHandlingTransaction(clientHandler, args)) return null;

        try {
            int expectedCount = Integer.parseInt(args[1]), timeout = Integer.parseInt(args[2]);
            List<Socket> replicas = new ArrayList<>(
                    ReplicaManager.replicaBuffers.keySet()
            );
            String getAckCommand = RESPEncoder.encodeArray(
                    new String[] {"REPLCONF", "GETACK", "*"}
            );
            Stream<CompletableFuture<Void>> futures = replicas.stream().map(
                    replica -> CompletableFuture.runAsync(() -> {
                        try {
                            OutputStream out = replica.getOutputStream();
                            out.write(getAckCommand.getBytes());
                            out.flush();
                        } catch (IOException e) {
                            throw new CompletionException(e);
                        }
                    })
            );

            try {
                CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }

            long startTime = System.currentTimeMillis();
            int ackedReplicas = 0;

            while (ackedReplicas < expectedCount) {
                long elapsedTime = System.currentTimeMillis() - startTime;
                if (elapsedTime >= timeout) {
                    break;
                }
                ackedReplicas = ReplConfImpl.getAckCount();
            }


            System.out.println("Final Acked replicas: " + ackedReplicas);
            ReplConfImpl.resetAckCount();

            response = RESPEncoder.encodeInteger(
                    ackedReplicas == 0 ? replicas.size() : ackedReplicas
            );
        } catch (NumberFormatException e) {
            response = RESPEncoder.encodeErrorString("ERR invalid arguments for 'WAIT'");
        }

        if (!invokeFromExec) {
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
        }
        return response;
    }
}
