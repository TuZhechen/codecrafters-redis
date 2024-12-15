package redis.commands.impl;

import redis.commands.RedisCommandHandler;
import redis.commands.TransactionHelper;
import redis.protocol.RESP.RESPEncoder;
import redis.replication.ReplicaManager;
import redis.server.ClientHandler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class PsyncImpl implements RedisCommandHandler {
    private final String replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

    @Override
    public void invoke(String[] args, ClientHandler clientHandler) throws IOException {
        if (TransactionHelper.isHandlingTransaction(clientHandler, args)) return;

        String response = RESPEncoder.encodeSimpleString("FULLRESYNC " + replid + " 0");
        clientHandler.getWriter().print(response);
        clientHandler.getWriter().flush();

        byte[] emptyRdb = createEmptyRdbFile();
        String lengthPrefix = RESPEncoder.encodeBulkStringLength(emptyRdb.length);

        OutputStream outputStream = clientHandler.getClientSocket().getOutputStream();
        outputStream.write(lengthPrefix.getBytes());
        outputStream.write(emptyRdb);
        outputStream.flush();

        startCommandPropagation(clientHandler);
    }

    private byte[] createEmptyRdbFile() {
        try (ByteArrayOutputStream rdbStream = new ByteArrayOutputStream()) {
            // RDB header: Magic string + version
            rdbStream.write("REDIS0011".getBytes());
            // RDB footer: End of file +  CRC64 checksum
            rdbStream.write(0xFF);
            rdbStream.write(longToBytes(0L));
            return rdbStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Unable to create RDB file", e);
        }
    }

    private byte[] longToBytes(long value) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(value);
        return buffer.array();
    }

    private void startCommandPropagation(ClientHandler clientHandler) {
        BlockingQueue<String> buffer = new LinkedBlockingQueue<>();
        ReplicaManager.addReplica(clientHandler.getClientSocket(), buffer);
        ReplicaManager.startCommandPropagator(clientHandler.getClientSocket(), buffer);
    }


}
