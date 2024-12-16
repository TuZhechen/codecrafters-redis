package redis.commands.impl;

import redis.commands.RedisCommandHandler;
import redis.commands.TransactionHelper;
import redis.protocol.RESP.RESPEncoder;
import redis.protocol.RESP.parsers.SimpleStringImpl;
import redis.server.ClientHandler;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class ReplConfImpl implements RedisCommandHandler {
    private static final AtomicInteger ackCount = new AtomicInteger(0);

    @Override
    public String invoke(String[] args, ClientHandler clientHandler, boolean invokeFromExec) {
        if (args[1].equalsIgnoreCase("ACK")) {
            if (args.length != 3) {
                String error = "ERR wrong number of arguments for 'ACK'";
                return TransactionHelper.errorResponse(clientHandler, error, invokeFromExec);
            }

            if (TransactionHelper.isHandlingTransaction(clientHandler, args)) return null;

            System.out.println("Received command: " + Arrays.toString(args));

            try {
                // Extract offset from the ACK command
                long offset = Long.parseLong(args[2]);
                ackCount.incrementAndGet();
                // Register this replica's ACK in the ReplicaManager
            } catch (NumberFormatException e) {
                System.err.println("Invalid ACK offset: " + args[2]);
            }

            return null;
        }
        else {
            String response = new SimpleStringImpl().encode("OK");
            if (!invokeFromExec) {
                clientHandler.getWriter().print(response);
                clientHandler.getWriter().flush();
            }
            return response;
        }
    }

    public static int getAckCount() {
        return ackCount.get();
    }

    public static void resetAckCount() {
        ackCount.set(0);
    }
}
