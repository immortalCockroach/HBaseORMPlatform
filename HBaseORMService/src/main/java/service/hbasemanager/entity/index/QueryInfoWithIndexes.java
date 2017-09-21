package service.hbasemanager.entity.index;

import com.immortalcockroach.hbaseorm.param.condition.Expression;
import com.immortalcockroach.hbaseorm.param.enums.ArithmeticOperatorEnum;
import service.constants.ServiceConstants;
import service.utils.ByteArrayUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 代表一个索引被部分或者全部命中,用于query查询时的使用
 */
public class QueryInfoWithIndexes {
    // 构成这个索引的列信息，和索引构建时列的顺序相同
    private List<Index> indexColumnList;

    // 查询列对应的表达式
    private Map<String, Expression> expressionMap;

    // 未被索引命中的查询条件
    private Set<String> unhitColumns;


    public QueryInfoWithIndexes(List<Index> indexColumnList, List<Expression> expressions, int[] hitNum) {
        this.indexColumnList = indexColumnList;
        expressionMap = new HashMap<>();

        for (Expression expression : expressions) {
            expressionMap.put(expression.getColumn(), expression);
        }

        Set<String> allColumns = new HashSet<>();
        // 根据hitNum将所有命中的索引加入到set中
        int size = hitNum.length;
        for (int i = 0; i <= size - 1; i++) {
            if (hitNum[i] == 0) {
                continue;
            }
            List<String> indexColumns = indexColumnList.get(i).getIndexColumnList();
            for (int j = 0; i <= hitNum[i] - 1; j++) {
                allColumns.add(indexColumns.get(j));
            }
        }
        // 如果查询条件未命中，则加入unHitColumns，等待回表查询
        for (String queryColumn : expressionMap.keySet()) {
            if (!allColumns.contains(queryColumn)) {
                unhitColumns.add(queryColumn);
            }
        }
    }

    public Set<String> getUnhitColumns() {
        return unhitColumns;
    }

    public List<Index> getIndexColumnList() {
        return indexColumnList;
    }

    public Map<String, Expression> getExpressionMap() {
        return expressionMap;
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
            // 索引只能用到第一个非等值查询为止
            if (expression.getArithmeticOperator() != ArithmeticOperatorEnum.EQ.getId()) {
                linePrefix.put(column, expression.getValues());
                qualifiers.add(column);
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
     *
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
                count++;
                break;
            } else {
                count++;
            }
        }
        return count;
    }

}