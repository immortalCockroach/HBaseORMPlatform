package service.hbasemanager.entity.scanparam;

import service.utils.ByteArrayUtils;

/**
 * Created by immortalCockroach on 10/5/17.
 */
public class KeyPair {
    private byte[] startKeys;
    private byte[] endKeys;

    public KeyPair(byte[] prefix) {
        this.startKeys = prefix;
        this.endKeys = ByteArrayUtils.getLargeByteArray(prefix);
    }

    public KeyPair(byte[] startKey, byte[] endKey) {
        this.startKeys = startKey;
        this.endKeys = endKey;
    }

    public byte[] getStartKeys() {
        return startKeys;
    }

    public void setStartKeys(byte[] startKeys) {
        this.startKeys = startKeys;
    }

    public byte[] getEndKeys() {
        return endKeys;
    }

    public void setEndKeys(byte[] endKeys) {
        this.endKeys = endKeys;
    }
}
