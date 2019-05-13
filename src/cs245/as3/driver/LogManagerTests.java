package cs245.as3.driver;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Test;

/**
 * DO NOT MODIFY THIS FILE IN THIS PACKAGE **
 */
public class LogManagerTests {

    private static class Record {
        long key;
        byte[] value;

        Record(long key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        public byte[] serialize() {
            ByteBuffer ret = ByteBuffer.allocate(Long.BYTES + value.length);
            ret.putLong(key);
            ret.put(value);
            return ret.array();
        }

        static Record deserialize(byte[] b) {
            ByteBuffer bb = ByteBuffer.wrap(b);
            long key = bb.getLong();
            byte[] value = new byte[b.length - Long.BYTES];
            bb.get(value);

            return new Record(key, value);
        }
    }

    @Test
    public void TestSimple() {
        LogManagerImpl lm = new LogManagerImpl();
        
        Record r = new Record(1, "foo".getBytes());
        byte[] r_byte = r.serialize();
        int r_len = r_byte.length;
        lm.appendLogRecord(r_byte);

        assert(lm.getLogEndOffset() == r_len);

        byte[] read_r = lm.readLogRecord(0, r_len);
        Record rec_r = Record.deserialize(read_r);

        assert(rec_r.key == r.key);
        assert(Arrays.equals(rec_r.value, r.value));

        Record r2 = new Record(2, "barbaz".getBytes());
        byte[] r2_byte = r2.serialize();
        int r2_len = r2_byte.length;
        lm.appendLogRecord(r2_byte);

        assert(lm.getLogEndOffset() == r_len + r2_len);
        byte[] read_r2 = lm.readLogRecord(r_len, r2_len);
        Record rec_r2 = Record.deserialize(read_r2);

        assert(rec_r2.key == r2.key);
        assert(Arrays.equals(rec_r2.value, r2.value));
    }
}
