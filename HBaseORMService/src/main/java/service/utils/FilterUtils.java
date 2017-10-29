package service.utils;

import com.immortalcockroach.hbaseorm.entity.query.Condition;
import com.immortalcockroach.hbaseorm.entity.query.Expression;
import com.immortalcockroach.hbaseorm.param.enums.ArithmeticOperatorEnum;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import service.hbasemanager.entity.scanparam.KeyPairsBuilder;
import service.hbasemanager.entity.tabldesc.TableDescriptor;

import java.util.ArrayList;
import java.util.List;


public class FilterUtils {

    public static Filter buildFilterListWithCondition(Condition condition, TableDescriptor descriptor) {
        List<Filter> filters = new ArrayList<>();
        List<Expression> expressions = condition.getExpressions();
        for (Expression expression : expressions) {
            List<Filter> columnFilters = getSingleColumnFilters(expression, descriptor);
            for (Filter filter : columnFilters) {
                filters.add(filter);
            }
        }
        return new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
    }

    private static List<Filter> getSingleColumnFilters(Expression expression, TableDescriptor descriptor) {
        int operatorId = expression.getArithmeticOperator();
        // 双目运算符
        if (ArithmeticOperatorEnum.isDoubleRange(operatorId)) {
            if (operatorId == ArithmeticOperatorEnum.BETWEEN.getId()) {
                this.keyPairList = KeyPairsBuilder.buildKeyPairsBetween(param, descriptor);
            } else if (operatorId == ArithmeticOperatorEnum.BETWEENL.getId()) {
                this.keyPairList = KeyPairsBuilder.buildKeyPairsBetweenL(param, descriptor);
            } else if (operatorId == ArithmeticOperatorEnum.BETWEENR.getId()) {
                this.keyPairList = KeyPairsBuilder.buildKeyPairsBetweenR(param, descriptor);
            } else if (operatorId == ArithmeticOperatorEnum.BETWEENLR.getId()) {
                this.keyPairList = KeyPairsBuilder.buildKeyPairsBetweenLR(param, descriptor);
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
            // 虽然在新的索引命中匹配算法的情况下
            this.keyPairList = KeyPairsBuilder.buildKeyPairsNEQ(param);
        }
    }

    private List<Filter>
}
