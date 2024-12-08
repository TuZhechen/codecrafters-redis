package redis.protocol.RESP.parsers;

public class IntegerImpl {
    public String encode(int rawValue) {
        return ":" + rawValue + "\r\n";
    }
    public String decode(String respString) {
        if (respString.startsWith(":")) {
            return respString.substring(1, respString.length() - 2);
        }
        throw new IllegalArgumentException("Invalid RESP Integer: " + respString);
    }
}
