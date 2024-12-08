package redis.protocol.RESP.parsers;

public class IntegerImpl {
    public String encode(String rawString) {
        return ":" + rawString + "\r\n";
    }
    public String decode(String respString) {
        if (respString.startsWith(":")) {
            return respString.substring(1, respString.length() - 2);
        }
        throw new IllegalArgumentException("Invalid RESP Integer: " + respString);
    }
}
