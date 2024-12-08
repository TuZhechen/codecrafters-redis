package redis.commands.impl;

import redis.commands.RedisCommandHandler;
import redis.protocol.RESP.parsers.SimpleStringImpl;
import redis.server.ClientHandler;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class ReplConfImpl implements RedisCommandHandler {
    private static final AtomicInteger ackCount = new AtomicInteger(0);

    @Override
    public void invoke(String[] args, ClientHandler clientHandler) {
        if (args[1].equalsIgnoreCase("ACK")) {
            if (args.length != 3) {
                String error = "-ERR wrong number of arguments for 'ACK'\r\n";
                clientHandler.getWriter().print(error);
                clientHandler.getWriter().flush();
                return;
            }
            System.out.println("Received command: " + Arrays.toString(args));

            try {
                // Extract offset from the ACK command
                long offset = Long.parseLong(args[2]);
                ackCount.incrementAndGet();
                // Register this replica's ACK in the ReplicaManager
            } catch (NumberFormatException e) {
                System.err.println("Invalid ACK offset: " + args[2]);
            }
        }
        else {
            String response = new SimpleStringImpl().encode("OK");
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
        }
    }

    public static int getAckCount() {
        return ackCount.get();
    }

    public static void resetAckCount() {
        ackCount.set(0);
    }
}
