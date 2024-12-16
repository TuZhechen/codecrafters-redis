package redis.protocol.RESP;

import redis.protocol.RESP.parsers.*;

public class RESPEncoder {
    public static String encodeSimpleString(String value) {
        return new SimpleStringImpl().encode(value);
    }

    public static String encodeErrorString(String value) {
        return new ErrorStringImpl().encode(value);
    }

    public static String encodeInteger(int value) {
        return new IntegerImpl().encode(value);
    }

    public static String encodeBulkString(String value) {
        return new BulkStringImpl().encode(value);
    }

    public static String encodeBulkStringLength(int value) {
        return new BulkStringImpl().encodeLength(value);
    }

    public static String encodeArray(Object[] elements) {
        return new ArraysImpl().encode(elements);
    }
}
