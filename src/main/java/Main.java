import redis.core.ServerConfig;
import redis.server.RedisServer;

public class Main {
    public static void main(String[] args) {
        System.out.println("Logs from your program will appear here!");
        ServerConfig serverConfig = new ServerConfig();
        serverConfig.parseArgs(args);

        RedisServer redisServer = new RedisServer(serverConfig);
        redisServer.start();
    }
}
