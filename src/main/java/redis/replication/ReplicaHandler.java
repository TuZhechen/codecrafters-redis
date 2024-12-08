package redis.replication;

import redis.core.MortalValue;
import redis.core.ServerConfig;
import redis.core.StorageManager;
import redis.protocol.RESP.RESPDecoder;
import redis.protocol.RESP.RESPEncoder;

import java.io.*;
import java.net.Socket;

public class ReplicaHandler {
    private final Socket masterSocket;
    private final ServerConfig serverConfig;
    private final StorageManager storageManager;
    private int offset = 0;

    public ReplicaHandler(ServerConfig serverConfig, StorageManager storageManager, Socket masterSocket) {
        this.serverConfig = serverConfig;
        this.storageManager = storageManager;
        this.masterSocket = masterSocket;
    }

    public void start() {
        try {
            // send a PING to the master
            OutputStream outputStream = masterSocket.getOutputStream();
            InputStream inputStream = masterSocket.getInputStream();
            // handshake 1/3
            sendPing(outputStream);
            readMasterResponse(inputStream);
            // handshake 2/3
            sendReplConf(outputStream, Integer.toString(serverConfig.getPort()));
            readMasterResponse(inputStream);
            readMasterResponse(inputStream);
            // handshake 3/3
            sendPsync(outputStream);
            readMasterResponse(inputStream);
            // skip rdb file transfer
            skipRdbFile(inputStream);
            Thread cmd = new Thread(() -> listenForCommands(inputStream, outputStream));
            cmd.start();
        } catch (IOException e) {
            System.err.println("Error initiating replica: " + e.getMessage());
        }
    }

    private void sendPing(OutputStream outputStream) throws IOException {
        System.out.println("Sending PING to master...");
        String pingRequest = RESPEncoder.encodeArray(new String[] {"PING"});
        outputStream.write(pingRequest.getBytes());
    }

    private void sendReplConf(OutputStream outputStream, String port) throws IOException {
        System.out.println("Sending REPL config to master...");
        String firstRequest = RESPEncoder.encodeArray(new String[] {"REPLCONF", "listening-port", port});
        outputStream.write(firstRequest.getBytes());
        String secondRequest = RESPEncoder.encodeArray(new String[] {"REPLCONF", "capa", "psync2"});
        outputStream.write(secondRequest.getBytes());
    }

    private void sendPsync(OutputStream outputStream) throws IOException {
        System.out.println("Sending PSYNC to master...");
        String request = RESPEncoder.encodeArray(new String[] {"PSYNC", "?", "-1"});
        outputStream.write(request.getBytes());
    }

    private void readMasterResponse(InputStream inputStream) throws IOException {
        byte[] buffer = new byte[56];
        int bytesRead = inputStream.read(buffer);

        if (bytesRead > 0) {
            String response = new String(buffer, 0 , bytesRead);
            System.out.println("Received response from master: " + response);
        } else {
            System.out.println("No bytes read from input stream");
        }
    }

    private void skipRdbFile(InputStream inputStream) throws IOException {
        // Read length prefix
        byte[] lengthBuffer = new byte[5];
        inputStream.read(lengthBuffer);
        String lengthPrefix = new String(lengthBuffer);

        if (!lengthPrefix.startsWith("$")) {
            System.out.println("Unexpected RDB length prefix: " + lengthPrefix);
            return;
        }

        // Parse RDB file length
        int rdbLength = Integer.parseInt(lengthPrefix.substring(1, lengthPrefix.indexOf('\r')));

        // Skip RDB content
        byte[] rdbBuffer = new byte[rdbLength];
        inputStream.read(rdbBuffer);
    }

    private void listenForCommands(InputStream inputStream, OutputStream outputStream)  {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        try {
            while (true) {
                String[] args = RESPDecoder.decodeCommand(reader);
                String respArrayString = RESPEncoder.encodeArray(args);
                if (args == null || args.length == 0) {
                    System.err.println("No commands received from master.");
                    break;
                }

                String command = args[0].toUpperCase();
                if ("REPLCONF".equals(command)) {
                    System.out.println("handling master command...");
                    handleReplConfCommand(args, outputStream);
                } else {
                    System.out.println("handling propagated command...");
                    handlePropagatedCommand(args);
                }
                offset += respArrayString.getBytes().length;
            }
        } catch (IOException e) {
            System.err.println("Error reading commands: " + e.getMessage());
        }
    }

    private void handlePropagatedCommand(String[] args) {
        String command = args[0].toUpperCase();
        switch (command) {
            case "SET":
                if (args.length < 3) {
                    System.err.println("Invalid SET from master, too few arguments");
                    return;
                }
                String key = args[1], value = args[2];
                storageManager.put(key, new MortalValue(value));
                break;
            default:
                break;
        }
    }

    private void handleReplConfCommand(String[] args, OutputStream outputStream) throws IOException {
        if ("GETACK".equalsIgnoreCase(args[1])) {
            String offsetStr = Integer.toString(offset);
            String resp = RESPEncoder.encodeArray(new String[] {"REPLCONF", "ACK", offsetStr});
            outputStream.write(resp.getBytes());
            outputStream.flush();
        } else {
            System.out.println("Unhandled REPLCONF command: " + args[1]);
        }
    }

}
