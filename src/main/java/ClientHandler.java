import java.io.*;
import java.net.*;

public class ClientHandler implements Runnable {
    private final Socket clientSocket;

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
