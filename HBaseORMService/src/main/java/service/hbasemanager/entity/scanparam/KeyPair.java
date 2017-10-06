package service.hbasemanager.entity.scanparam;

import service.utils.ByteArrayUtils;

/**
 * Created by immortalCockroach on 10/5/17.
 */
public class KeyPair {
    private byte[] startKey;
    private byte[] endKey;

    public KeyPair(byte[] prefix) {
        this.startKey = prefix;
        this.endKey = ByteArrayUtils.getLargeByteArray(prefix);
    }

    public KeyPair(byte[] startKey, byte[] endKey) {
        this.startKey = startKey;
        this.endKey = endKey;
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public void setStartKey(byte[] startKey) {
        this.startKey = startKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }

    public void setEndKey(byte[] endKey) {
        this.endKey = endKey;
    }
}
