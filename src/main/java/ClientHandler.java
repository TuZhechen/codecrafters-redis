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
            while((request = reader.readLine()) != null) {
                if ("PING".equalsIgnoreCase(request)) {
                    outputStream.write("+PONG\r\n".getBytes());
                } else if("ECHO".equalsIgnoreCase(request)) {
                    reader.readLine();
                    String echo = reader.readLine();
                    outputStream.write(
                            String.format("$%d\r\n%s\r\n", echo.length(), echo).getBytes()
                    );
                } else if("SET".equalsIgnoreCase(request)) {
                    reader.readLine();
                    String key = reader.readLine();
                    reader.readLine();
                    String value = reader.readLine();
                    MortalValue mortalValue = new MortalValue(value);
                    System.out.println("after value: " + reader.readLine());
                    if("PX".equalsIgnoreCase(reader.readLine())) {
                        System.out.println("after px: " + reader.readLine());
                        int lifespan = Integer.parseInt(reader.readLine());
                        mortalValue.setExpirationTime(System.currentTimeMillis() + lifespan);
                    }
                    System.out.println("mortal value before store: " + mortalValue.getValue());
                    synchronized (store) {
                        store.put(key, mortalValue);
                    }
                    System.out.println("mortal value after store: " + mortalValue.getValue());
                    outputStream.write("+OK\r\n".getBytes());
                } else if("GET".equalsIgnoreCase(request)) {
                    reader.readLine();
                    String key = reader.readLine();
                    MortalValue mortalValue;
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
}
