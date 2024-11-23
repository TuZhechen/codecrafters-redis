import java.io.*;
import java.net.*;
import java.util.HashMap;

public class ClientHandler implements Runnable {
    private final Socket clientSocket;
    private static final HashMap<String, MortalValue> store = new HashMap<>();

    public ClientHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        try {
            InputStream inputStream = clientSocket.getInputStream();
            OutputStream outputStream = clientSocket.getOutputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String request;
            while ((request = reader.readLine()) != null) {
                System.out.println("Raw request: " + request);
                if (request.startsWith("*")) {
                    int argCount = Integer.parseInt(request.substring(1));
                    String[] args = new String[argCount];
                    for (int i = 0; i < argCount; i++) {
                        reader.readLine();
                        args[i] = reader.readLine();
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
                            default:
                                outputStream.write("-ERR unknown command\r\n".getBytes());

                    }
                }
            }
        } catch (IOException e) {
            System.out.println("IOException when handling client: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.out.println("IOException when closing client " + e.getMessage());
            }
        }
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
    }

    private void handleGet(String[] args, OutputStream outputStream) throws IOException {
        MortalValue mortalValue;
        if (args.length < 2) {
            outputStream.write("-ERR wrong number of arguments for 'GET'\r\n".getBytes());
            return;
        }
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
}
