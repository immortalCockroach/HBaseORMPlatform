package service.hbasemanager.entity.indexresult;

import com.immortalcockroach.hbaseorm.param.condition.Expression;
import service.utils.ByteArrayUtils;

/**
 * 根据查询的condition 构造scan的startKey，endKey，filter等
 */
public class TableScanParam {
    private byte[] startKey;
    private byte[] endKey;

    /**
     * 非等值查询命中的情况
     * @param prefix
     * @param expression
     */
    public TableScanParam(byte[] prefix, Expression expression) {
    }

    /**
     * 等值查询命中的情况
     * @param prefix
     */
    public  TableScanParam(byte[] prefix) {
        // 如果是等值的查询，取startKey为前缀，endKey为比startKey大1即可
        this.startKey = prefix;
        this.endKey = ByteArrayUtils.getLargeByteArray(startKey);

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
