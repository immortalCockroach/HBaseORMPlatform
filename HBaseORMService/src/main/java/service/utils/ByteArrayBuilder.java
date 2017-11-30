package service.utils;

/**
 *
 */
public class ByteArrayBuilder {
    private int pos;
    private byte[] store;

    public ByteArrayBuilder(int size) {
        store = new byte[size];
        pos = 0;
    }

    public void put(byte[] src) {
        System.arraycopy(src, 0, store, pos, src.length);
        pos += src.length;
    }

    public void put(byte b) {
        store[pos] = b;
        pos++;
    }

    public int getPosition() {
        return pos;
    }

    public void setPosition(int newPosition) {
        pos = newPosition;
    }

    public void addPosition(int bias) {
        pos += bias;
    }

    public byte[] array() {
        return store;
    }
}
