package service.hbasemanager.entity.scanparam;

import com.immortalcockroach.hbaseorm.entity.query.Expression;
import com.immortalcockroach.hbaseorm.param.enums.ArithmeticOperatorEnum;
import service.hbasemanager.entity.tabldesc.TableDescriptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 根据查询的condition 构造scan的startKey，endKey，filter等
 */
public final class TableScanParam {
    // 多个startKey和endKey的组合，因为可能有多次查询
    private List<KeyPair> keyPairList;

    // 代表扫描的条件是否有效，例如 > Integer.MAX就是无效查询
    private boolean isValid;

    /**
     * 非等值查询的情况，根据expression来构造startKey和endKey
     *
     * @param linePrefix
     * @param qualifiers
     * @param expression
     * @param indexNum
     */
    public TableScanParam(Map<String, byte[]> linePrefix, List<String> qualifiers, Expression expression, byte
            indexNum, TableDescriptor descriptor) {
        // 根据expression的情况设置startKey和endKey
        int operatorId = expression.getArithmeticOperator();
        IndexParam param = new IndexParam(linePrefix, qualifiers, expression, indexNum);
        // 双目运算符
        if (ArithmeticOperatorEnum.isDoubleRange(operatorId)) {
            if (operatorId == ArithmeticOperatorEnum.BETWEEN.getId()) {
                this.keyPairList = KeyPairsBuilder.buildKeyPairsBetween(param, descriptor);
            } else if (operatorId == ArithmeticOperatorEnum.BETWEENL.getId()) {
                this.keyPairList = KeyPairsBuilder.buildKeyPairsBetweenL(param, descriptor);
            } else if (operatorId == ArithmeticOperatorEnum.BETWEENR.getId()) {

            } else if (operatorId == ArithmeticOperatorEnum.BETWEENLR.getId()){

            }
        } else if (ArithmeticOperatorEnum.isSingleRange(operatorId)) { //
            // 单目运算符

            // 单个的情况下 只取一边的
            if (operatorId == ArithmeticOperatorEnum.GT.getId()) {

                this.keyPairList = KeyPairsBuilder.buildKeyPairsGT(param, descriptor);
            } else if (operatorId == ArithmeticOperatorEnum.GE.getId()) {
                // 大于等于的情况  endKey为不包含运算符的greater
                this.keyPairList = KeyPairsBuilder.buildKeyPairsGE(param, descriptor);
            } else if (operatorId == ArithmeticOperatorEnum.LT.getId()) {
                // 小于的情况  startKey为
                this.keyPairList = KeyPairsBuilder.buildKeyPairsLT(param, descriptor);
            } else if (operatorId == ArithmeticOperatorEnum.LE.getId()) {
                this.keyPairList = KeyPairsBuilder.buildKeyPairsLE(param, descriptor);
            }
        } else {
            // 不等于的情况下必须扫索引表的该前缀的全部，后面的再过滤
            this.keyPairList = KeyPairsBuilder.buildKeyPairsNEQ(param);
        }

        // 如果返回是null 代表不合法
        if (keyPairList == null) {
            this.isValid = false;
        }
    }


    /**
     * 等值查询命中的情况，此处直接设置startKey和endKey即可。
     *
     * @param prefix
     */
    public TableScanParam(byte[] prefix) {
        // 如果是等值的查询，取startKey为前缀，endKey为比startKey大1即可
        this.keyPairList = new ArrayList<>();
        keyPairList.add(new KeyPair(prefix));
        this.isValid = true;
    }

    public List<KeyPair> getKeyPairList() {
        return keyPairList;
    }

    public void setKeyPairList(List<KeyPair> keyPairList) {
        this.keyPairList = keyPairList;
    }

    public boolean isValid() {
        return isValid;
    }

    public void setValid(boolean valid) {
        isValid = valid;
    }
}
