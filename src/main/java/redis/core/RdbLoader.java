package redis.core;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class RdbLoader {
    private static final String RDB_HEADER = "REDIS";

    public static void loadRdbFile(String dir, String dbfilename, Map<String, MortalValue> store) {
        Path rdbFile = Paths.get(dir, dbfilename);

        if (!Files.exists(rdbFile)) {
            System.out.println("File not found: " + rdbFile);
            return;
        }

        try (DataInputStream inputStream = new DataInputStream(Files.newInputStream(rdbFile))) {
            // Step 1: Validate the header
            validateHeader(inputStream);

            // Step 2: Parse the file
            while (true) {
                int type = inputStream.readUnsignedByte();
                if (type == 0xFF) { // End of file marker
                    break;
                }

                switch (type) {
                    case 0xFA: // Metadata section
                        skipMetadata(inputStream);
                        break;
                    case 0xFE: // Start of a database subsection
                        parseDatabaseSubsection(inputStream);
                        break;
                    case 0xFB: // Hash table size
                        skipHashTableSize(inputStream);
                        break;
                    case 0x00: // String key-value pair
                        parseStringKeyValuePair(inputStream, store);
                        break;
                    case 0xFC: // Expiry timestamp in milliseconds
                        parseExpiryMilliseconds(inputStream, store);
                        break;
                    case 0xFD: // Expiry timestamp in seconds
                        parseExpirySeconds(inputStream, store);
                        break;
                    default:
                        throw new IOException("Unsupported entry type: " + type);
                }
            }

            System.out.println("RDB file loaded successfully.");
        } catch (IOException e) {
            System.err.println("Error reading RDB file: " + e.getMessage());
        }
    }

    private static void validateHeader(DataInputStream in) throws IOException {
        byte[] header = new byte[5];
        in.readFully(header);
        if (!new String(header).equals(RDB_HEADER)) {
            throw new IOException("Invalid header format");
        }
        in.skipBytes(4); // skip redis version
    }

    private static void skipMetadata(DataInputStream in) throws IOException {
        String name = readString(in);
        String value = readString(in);
        System.out.println("Metadata skipped: " + name + " -> " + value);
    }

    private static void skipHashTableSize(DataInputStream in) throws IOException {
        int permanentSize = (int) readLength(in);
        int expirySize = (int) readLength(in);
        System.out.println("Key-value Hash Table Size: " + permanentSize);
        System.out.println("Expiry Hash Table Size: " + expirySize);
    }

    private static void parseDatabaseSubsection(DataInputStream in) throws IOException {
        System.out.println("Database subsection:" + (int) readLength(in));
    }

    private static void parseStringKeyValuePair(DataInputStream in, Map<String, MortalValue> store) throws IOException {
        String key = readString(in);
        String value = readString(in);
        store.put(key, new MortalValue(value));
        System.out.println("Key: " + key + " Value: " + value);
    }

    private static void parseExpiryMilliseconds(DataInputStream in, Map<String, MortalValue> store) throws IOException {
        long ttl = readLittleEndianLong(in);
        int valueType = in.readUnsignedByte();
        if (valueType != 0x00) {
            throw new IOException("Unsupported value type: " + valueType);
        }
        String key = readString(in);
        String value = readString(in);
        MortalValue mortalValue = new MortalValue(value);
        mortalValue.setExpirationTime(ttl);
        store.put(key, mortalValue);
        System.out.println("Key: " + key + " Value: " + value + "Expired at: " + ttl);
    }

    private static void parseExpirySeconds(DataInputStream in, Map<String, MortalValue> store) throws IOException {
        int ttl = readLittleEndianInt(in);
        long ttlms = ttl * 1000L;
        int valueType = in.readUnsignedByte();
        if (valueType != 0x00) {
            throw new IOException("Unsupported value type: " + valueType);
        }
        String key = readString(in);
        String value = readString(in);
        MortalValue mortalValue = new MortalValue(value);
        mortalValue.setExpirationTime(ttlms);
        store.put(key, mortalValue);
        System.out.println("Key: " + key + " Value: " + value + "Expired at: " + ttlms);
    }

    private static Object readLength(DataInputStream in) throws IOException {
        int firstByte = in.readUnsignedByte();
        int indicator = firstByte & 0xC0;

        System.out.printf("readLength: firstByte=0x%02X, indicator=0x%02X%n", firstByte, indicator);

        // Case 1: read 6-bit size
        int lengthType = firstByte & 0x3F;
        if (indicator == 0x00) {
            // 00xxxxxx
            System.out.println("6-bit length: " + lengthType);
            return lengthType;

            // Case 2: read 14-bit size
        } else if (indicator == 0x40) {
            // 01xxxxxx
            int secondByte = in.readUnsignedByte();
            int len = (lengthType) << 8 | secondByte;
            System.out.println("14-bit length: " + len);
            return len;

            // Case 3: skip the remaining bits read the next 32-bit size
        } else if (indicator == 0x80) {
            // 10xxxxxx
            int len = in.readInt();
            System.out.println("32-bit length: " + len);
            return len;

        } else if (indicator == 0xC0) {
            // 11xxxxxx
            System.out.println("C0 case");
            return new SpecialEncoding(lengthType);
        } else {
            throw new IOException("Invalid size encoding: " + indicator);
        }
    }

    private static String readString(DataInputStream in) throws IOException {
        Object lengthOrEncoding = readLength(in);

        if (lengthOrEncoding instanceof Integer) {
            int length = (int) lengthOrEncoding;
            System.out.println("Reading normal string of length: " + length);
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            String result = new String(bytes);
            System.out.println("Read normal string: " + result);
            return result;
        } else if (lengthOrEncoding instanceof SpecialEncoding) {
            int typeOfString = ((SpecialEncoding) lengthOrEncoding).getType();
            System.out.println("Reading number string of type: " + typeOfString);
            switch (typeOfString) {
                case 0x00: // 8-bit
                    return String.valueOf(in.readByte());
                case 0x01: // 16-bit
                    return String.valueOf(readLittleEndianShort(in));
                case 0x02: // 32-bit
                    return String.valueOf(readLittleEndianInt(in));
                case 0x03:
                    throw new IOException("Compressed strings not supported");
                default:
                    throw new IOException("Unsupported entry type: " + typeOfString);
            }
        } else {
            throw new IOException("Unknown length encoding");
        }
    }

    private static short readLittleEndianShort(DataInputStream inputStream) throws IOException {
        byte[] buffer = new byte[2];
        inputStream.readFully(buffer);
        ByteBuffer wrapped = ByteBuffer.wrap(buffer).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        return wrapped.getShort();
    }

    private static int readLittleEndianInt(DataInputStream inputStream) throws IOException {
        byte[] buffer = new byte[4];
        inputStream.readFully(buffer);
        ByteBuffer wrapped = ByteBuffer.wrap(buffer).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        return wrapped.getInt();
    }

    private static long readLittleEndianLong(DataInputStream inputStream) throws IOException {
        byte[] buffer = new byte[8];
        inputStream.readFully(buffer);
        ByteBuffer wrapped = ByteBuffer.wrap(buffer).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        return wrapped.getLong();
    }
}

