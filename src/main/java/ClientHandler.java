import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

public class ClientHandler implements Runnable {
    private final Socket clientSocket;
    private final Map<String, MortalValue> store;
    private final Map<String, String> config;

    public ClientHandler(Socket clientSocket, Map<String, String> config, Map<String, MortalValue> store) {
        this.clientSocket = clientSocket;
        this.config = config;
        this.store = store;
    }

    @Override
    public void run() {
        try {
            InputStream inputStream = clientSocket.getInputStream();
            OutputStream outputStream = clientSocket.getOutputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            while (!clientSocket.isClosed()) {
                try {
                    String[] args = parseRespCommand(reader);
                    if (args == null || args.length == 0) {
                        break;
                    }
                    String command = args[0].toUpperCase();
                    switch (command) {
                        case "PING":
                            outputStream.write("+PONG\r\n".getBytes());
                            break;
                        case "ECHO":
                            if (args.length > 1) {
                                String echo = args[1];
                                outputStream.write(
                                        String.format("$%d\r\n%s\r\n", echo.length(), echo).getBytes()
                                );
                            } else {
                                outputStream.write("-ERR wrong number of arguments for 'ECHO'\r\n".getBytes());
                            }
                            break;
                        case "SET":
                            handleSet(args, outputStream);
                            break;
                        case "GET":
                            handleGet(args, outputStream);
                            break;
                        case "CONFIG":
                            handleConfigGet(args, outputStream);
                            break;
                        case "KEYS":
                            handleKeys(args, outputStream);
                            break;
                        case "INFO":
                            handleInfo(args, outputStream);
                            break;
                        case "REPLCONF":
                            outputStream.write("+OK\r\n".getBytes());
                            break;
                        case "PSYNC":
                            handPsync(outputStream, clientSocket);
                            break;
                            default:
                                outputStream.write("-ERR unknown command\r\n".getBytes());
                    }
                } catch (IOException e) {
                    outputStream.write(("-ERR" + e.getMessage() + "\r\n").getBytes());
                }
            }
        } catch (IOException e) {
            System.out.println("IOException when handling client: " + e.getMessage());
        } finally {
            try {
                if (!clientSocket.isClosed()) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException when closing client " + e.getMessage());
            }
        }
    }

    public static String[] parseRespCommand(BufferedReader reader) throws IOException {
        String req = reader.readLine();
        System.out.println("Raw first input: " + req);
        if (req == null) {
            throw new IOException("Invalid RESP command: empty or null input");
        }
        if (!req.startsWith("*")) {
            throw new IOException("Invalid RESP command: wrong head");
        }
        int argCount = Integer.parseInt(req.substring(1));
        String[] args = new String[argCount];
        for (int i = 0; i < argCount; i++) {
            String bulkLength = reader.readLine();
            if (bulkLength == null || !bulkLength.startsWith("$")) {
                throw new IOException("Invalid RESP command: wrong length");
            }

            String arg = reader.readLine();
            if (arg == null) {
                throw new IOException("Invalid RESP command: missing arg");
            }
            args[i] = arg;
        }
        return args;
    }


    private void handleSet(String[] args, OutputStream outputStream) throws IOException {
        if (args.length < 3) {
            outputStream.write("-ERR wrong number of arguments for 'SET'\r\n".getBytes());
        }
        String key = args[1], value = args[2];
        MortalValue mortalValue = new MortalValue(value);
        if(args.length == 5 && "PX".equalsIgnoreCase(args[3])) {
            try {
                int lifespan = Integer.parseInt(args[4]);
                mortalValue.setExpirationTime(System.currentTimeMillis() + lifespan);
            } catch (NumberFormatException e) {
                outputStream.write("-ERR invalid PX value\r\n".getBytes());
            }
        }
        synchronized (store) {
            store.put(key, mortalValue);
        }
        outputStream.write("+OK\r\n".getBytes());
        String command = String.format("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
                                       key.length(), key, value.length(), value);
        for (BlockingQueue<String> buffer : Main.replicaBuffers.values()) {
            buffer.offer(command);
        }
    }

    private void handleGet(String[] args, OutputStream outputStream) throws IOException {
        if (args.length < 2) {
            outputStream.write("-ERR wrong number of arguments for 'GET'\r\n".getBytes());
            return;
        }
        MortalValue mortalValue;
        String key = args[1];
        synchronized (store) {
            mortalValue = store.get(key);
        }
        if (mortalValue != null && !mortalValue.isExpired()) {
            String value = mortalValue.getValue();
            outputStream.write(
                    String.format("$%d\r\n%s\r\n", value.length(), value).getBytes()
            );
        } else {
            outputStream.write("$-1\r\n".getBytes());
        }
    }

    private void handleConfigGet(String[] args, OutputStream outputStream) throws IOException {
        if (args.length < 3) {
            outputStream.write("-ERR wrong number of arguments for 'CONFIG'".getBytes());
            return;
        }
        String option = args[1], name = args[2];
        if("GET".equalsIgnoreCase(option)) {
            System.out.println("Getting " + name + "...");
            String value = config.get(name);
            String response = String.format(
                    "*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
                    name.length(), name, value.length(), value
            );
            outputStream.write(response.getBytes());
        }
    }

    private void handleKeys(String[] args, OutputStream outputStream) throws IOException {
        if (args.length < 2) {
            outputStream.write("-ERR KEYS requires at least one pattern argument\r\n".getBytes());
            return;
        }

        String pattern = args[1];
        System.out.println("Fetching keys like " + pattern + "...");
        String regex = pattern.replace("*", ".*");

        List<String> matchedKeys;
        synchronized (store) {
            matchedKeys = store.keySet().stream()
                    .filter(key -> key.matches(regex))
                    .toList();
        }

        if (matchedKeys.isEmpty()) {
            outputStream.write("*0\r\n".getBytes());
        } else {
            StringBuilder response = new StringBuilder();
            response.append("*").append(matchedKeys.size()).append("\r\n");
            for (String key : matchedKeys) {
                response.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
            }
            outputStream.write(response.toString().getBytes());
        }
    }

    private void handleInfo(String[] args, OutputStream outputStream) throws  IOException {
        String option = args[1];
        String info;
        if ("slave".equalsIgnoreCase(config.get("role"))) {
            info = "role:slave";
        } else {
            String replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
            info = "role:master\n" +
                    "master_replid:" + replid + "\n" +
                    "master_repl_offset:0"
                    ;
        }
        String response = String.format(
                "$%d\r\n%s\r\n", info.length(), info
        );
        outputStream.write(response.getBytes());
    }

    private void handPsync(OutputStream outputStream, Socket replicaSocket) throws IOException {
        String replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
        String resp = "+FULLRESYNC " + replid + " 0\r\n";
        outputStream.write(resp.getBytes());
        byte[] emptyRdb = createEmptyRdbFile();
        String lengthPrefix = "$" + emptyRdb.length + "\r\n";
        outputStream.write(lengthPrefix.getBytes());
        outputStream.write(emptyRdb);
        outputStream.flush();
        System.out.println("Start command propagation...");
        BlockingQueue<String> buffer = new LinkedBlockingQueue<>();
        Main.replicaBuffers.put(replicaSocket, buffer);
        Main.startCommandPropagator(replicaSocket, buffer);
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
}
