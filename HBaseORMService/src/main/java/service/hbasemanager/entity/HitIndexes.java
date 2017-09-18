package service.hbasemanager.entity;

import com.immortalcockroach.hbaseorm.param.condition.Expression;
import com.immortalcockroach.hbaseorm.param.enums.ArithmeticOperatorEnum;
import service.constants.ServiceConstants;
import service.utils.ByteArrayUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 代表一个索引被部分或者全部命中,用于query查询时的使用
 */
public class HitIndexes {
    // 构成这个索引的列信息，和索引构建时列的顺序相同
    private List<Index> indexColumnList;

    // 查询列对应的表达式
    private Map<String, Expression> expressionMap;

    // 剩余的列，需要回表查询
    private Set<String> leftColumns;

    public HitIndexes(List<Index> indexColumnList, List<Expression> expressions, int[] hitNum, String[] qualifiers) {
        this.indexColumnList = indexColumnList;
        expressionMap = new HashMap<>();
        leftColumns = new HashSet<>(Arrays.asList(qualifiers));

        Set<String> indexColumns = new HashSet<>();
        for (Expression expression : expressions) {
            expressionMap.put(expression.getColumn(), expression);
        }

        // 查询的列和命中的index的所有列的差集即为需要回表查询的列
        int size = hitNum.length;
        for (int i = 0; i <= size - 1; i++) {
            // 非命中的index跳过
            if (hitNum[i] == 0) {
                continue;
            }
            indexColumns.addAll(indexColumnList.get(i).getIndexColumnList());
        }
        leftColumns.removeAll(indexColumnList);
    }

    /**
     * 根据命中的索引信息，构建索引表查询的前缀
     * i位表索引的序号，hitNum为该索引命中的前缀数量
     *
     * @return
     */
    public byte[] buildIndexTableQueryPrefix(int i, int hitNum) {
        Index index = indexColumnList.get(i);
        Map<String, byte[]> linePrefix = new HashMap<>();
        List<String> qualifiers = new ArrayList<>();
        for (int j = 0; j <= hitNum - 1; j++) {
            String column = index.getIndexColumnList().get(j);
            Expression expression = expressionMap.get(column);
            // 索引只能用到最后一个等值查询为止
            if (expression.getArithmeticOperator() != ArithmeticOperatorEnum.EQ.getId()) {
                break;
            } else {
                linePrefix.put(column, expression.getValues());
                qualifiers.add(column);
            }
        }
        return ByteArrayUtils.generateIndexRowKey(linePrefix, qualifiers.toArray(new String[]{}), ServiceConstants.EOT, ServiceConstants.ESC, ServiceConstants.NUL, (byte) index.getIndexNum());
    }

    /**
     * 命中索引中等值的前缀的数量
     * @param i
     * @param hitNum
     * @return
     */
    public int getPrefixCount(int i, int hitNum) {
        Index index = indexColumnList.get(i);

        int count = 0;
        for (int j = 0; j <= hitNum - 1; j++) {
            Expression expression = expressionMap.get(index.getIndexColumnList().get(j));
            // 索引只能用到最后一个等值查询为止
            if (expression.getArithmeticOperator() != ArithmeticOperatorEnum.EQ.getId()) {
                break;
            } else {
                count++;
            }
        }
        return count;
    }

}
