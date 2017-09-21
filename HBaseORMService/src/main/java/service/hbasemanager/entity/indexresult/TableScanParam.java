package service.hbasemanager.entity.indexresult;

/**
 * 根据查询的condition 构造scan的startKey，endKey，filter等
 */
public class TableScanParam {
    private byte[] startKey;
    private byte[] endKey;
    private byte[] prefix;

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

    public byte[] getPrefix() {
        return prefix;
    }

    public void setPrefix(byte[] prefix) {
        this.prefix = prefix;
    }
}
