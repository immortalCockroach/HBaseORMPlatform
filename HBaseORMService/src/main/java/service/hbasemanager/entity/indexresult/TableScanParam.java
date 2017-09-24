package service.hbasemanager.entity.indexresult;

import com.immortalcockroach.hbaseorm.param.condition.Expression;
import com.immortalcockroach.hbaseorm.param.enums.ArithmeticOperatorEnum;
import service.utils.ByteArrayUtils;

import java.util.List;
import java.util.Map;

/**
 * 根据查询的condition 构造scan的startKey，endKey，filter等
 */
public final class TableScanParam {
    private byte[] startKey;
    private byte[] endKey;

    /**
     * 非等值查询的情况，根据expression来构造startKey和endKey
     *
     * @param linePrefix
     * @param qualifiers
     * @param expression
     * @param indexNum
     */
    public TableScanParam(Map<String, byte[]> linePrefix, List<String> qualifiers, Expression expression, byte indexNum) {
        // 根据expression的情况设置startKey和endKey
        int operatorId = expression.getArithmeticOperator();

        if (ArithmeticOperatorEnum.isDoubleRange(operatorId)) {

        } else if (ArithmeticOperatorEnum.isSingleRange(operatorId)) {
            // 单个的情况下 只取一边的
            if (operatorId == ArithmeticOperatorEnum.GT.getId()) {
                // 大于的情况  endKey为不包含运算符的greater
                this.endKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.generateIndexRowKey(linePrefix, qualifiers.toArray(new String[]{}), indexNum));
                // 取startKey为包含不等运算符的greater
                String column = expression.getColumn();
                byte[] value = expression.getValue();
                linePrefix.put(column, value);
                qualifiers.add(column);
                this.startKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.generateIndexRowKey(linePrefix, qualifiers.toArray(new String[]{}), indexNum));
            } else if (operatorId == ArithmeticOperatorEnum.GE.getId()) {
                // 大于等于的情况  endKey为不包含运算符的greater
                this.endKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.generateIndexRowKey(linePrefix, qualifiers.toArray(new String[]{}), indexNum));
                // 取startKey为包含不等运算符的前缀
                String column = expression.getColumn();
                byte[] value = expression.getValue();
                linePrefix.put(column, value);
                qualifiers.add(column);
                this.startKey = (ByteArrayUtils.generateIndexRowKey(linePrefix, qualifiers.toArray(new String[]{}), indexNum));
            } else if (operatorId == ArithmeticOperatorEnum.LT.getId()) {
                // 小于的情况  startKey为
                this.endKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.generateIndexRowKey(linePrefix, qualifiers.toArray(new String[]{}), indexNum));
                // 取startKey为包含不等运算符的前缀
                String column = expression.getColumn();
                byte[] value = expression.getValue();
                linePrefix.put(column, value);
                qualifiers.add(column);
                this.startKey = (ByteArrayUtils.generateIndexRowKey(linePrefix, qualifiers.toArray(new String[]{}), indexNum));
            } else if (operatorId == ArithmeticOperatorEnum.LE.getId()) {

            }
        } else {
            // 不等于的情况下必须扫索引表的一部分，后面的再过滤
            this.startKey = ByteArrayUtils.generateIndexRowKey(linePrefix, qualifiers.toArray(new String[]{}), indexNum);
            this.endKey = ByteArrayUtils.getLargeByteArray(startKey);
        }
    }

    /**
     * 等值查询命中的情况，此处直接设置startKey和endKey即可。
     *
     * @param prefix
     */
    public TableScanParam(byte[] prefix) {
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
