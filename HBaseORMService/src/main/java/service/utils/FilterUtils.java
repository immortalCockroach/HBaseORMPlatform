package service.utils;

import com.immortalcockroach.hbaseorm.constant.CommonConstants;
import com.immortalcockroach.hbaseorm.entity.query.Condition;
import com.immortalcockroach.hbaseorm.entity.query.Expression;
import com.immortalcockroach.hbaseorm.param.enums.ArithmeticOperatorEnum;
import com.immortalcockroach.hbaseorm.util.Bytes;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.LongComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import service.constants.ServiceConstants;
import service.hbasemanager.entity.comparators.ByteComparator;
import service.hbasemanager.entity.comparators.IntComparator;
import service.hbasemanager.entity.comparators.ShortComparator;
import service.hbasemanager.entity.tabldesc.TableDescriptor;

import java.util.ArrayList;
import java.util.List;


public class FilterUtils {

    public static Filter buildFilterListWithCondition(Condition condition, TableDescriptor descriptor) {
        List<Filter> filters = new ArrayList<>();
        List<Expression> expressions = condition.getExpressions();
        for (Expression expression : expressions) {
            // 行健不构建filter
            if (expression.getColumn().equals(CommonConstants.ROW_KEY)) {
                continue;
            }
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
                return buildFilterBetween(expression, descriptor);
            } else if (operatorId == ArithmeticOperatorEnum.BETWEENL.getId()) {
                return buildFilterBetweenL(expression, descriptor);
            } else if (operatorId == ArithmeticOperatorEnum.BETWEENR.getId()) {
                return buildFilterBetweenR(expression, descriptor);
            } else if (operatorId == ArithmeticOperatorEnum.BETWEENLR.getId()) {
                return buildFilterBetweenLR(expression, descriptor);
            }
        } else if (ArithmeticOperatorEnum.isSingleRange(operatorId)) { //
            // 单目运算符

            // 单个的情况下 只取一边的
            if (operatorId == ArithmeticOperatorEnum.GT.getId()) {

                return buildFilterGT(expression, descriptor);
            } else if (operatorId == ArithmeticOperatorEnum.GE.getId()) {
                // 大于等于的情况  endKey为不包含运算符的greater
                return buildFilterGE(expression, descriptor);
            } else if (operatorId == ArithmeticOperatorEnum.LT.getId()) {
                // 小于的情况  startKey为
                return buildFilterLT(expression, descriptor);
            } else if (operatorId == ArithmeticOperatorEnum.LE.getId()) {
                return buildFilterLE(expression, descriptor);
            }
        } else {
            // 不等于的情况下必须扫索引表的该前缀的全部，后面的再过滤
            // 虽然在新的索引命中匹配算法的情况下
            return buildFilterNEQ(expression, descriptor);
        }
        return null;
    }

    private static List<Filter> buildFilterBetween(Expression expression, TableDescriptor descriptor) {
        String column = expression.getColumn();
        Integer type = descriptor.getTypeOfColumn(column);
        byte[] left = expression.getValue();
        byte[] right = expression.getOptionValue();
        List<Filter> filters = new ArrayList<>();
        switch (type) {
            case 0: // string

                SingleColumnValueFilter filterStr1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER, left);

                SingleColumnValueFilter filterStr2 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS, right);

                filters.add(filterStr1);
                filters.add(filterStr2);
                return filters;
            case 1: // byte
                SingleColumnValueFilter filterB1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER, new ByteComparator(left[0]));

                SingleColumnValueFilter filterB2 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS, new ByteComparator(right[0]));

                filters.add(filterB1);
                filters.add(filterB2);
                return filters;
            case 2: // short
                SingleColumnValueFilter filterS1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER, new ShortComparator(Bytes.toShort(left)));

                SingleColumnValueFilter filterS2 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS, new ShortComparator(Bytes.toShort(right)));

                filters.add(filterS1);
                filters.add(filterS2);
                return filters;
            case 3: // int
                SingleColumnValueFilter filterI1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER, new IntComparator(Bytes.toInt(left)));

                SingleColumnValueFilter filterI2 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS, new IntComparator(Bytes.toInt(right)));

                filters.add(filterI1);
                filters.add(filterI2);
                return filters;
            case 4: // long
                SingleColumnValueFilter filterL1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER, new LongComparator(Bytes.toLong(left)));

                SingleColumnValueFilter filterL2 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS, new LongComparator(Bytes.toLong(right)));

                filters.add(filterL1);
                filters.add(filterL2);
                return filters;
            default:
                return null;
        }
    }

    private static List<Filter> buildFilterBetweenL(Expression expression, TableDescriptor descriptor) {
        String column = expression.getColumn();
        Integer type = descriptor.getTypeOfColumn(column);
        byte[] left = expression.getValue();
        byte[] right = expression.getOptionValue();
        List<Filter> filters = new ArrayList<>();
        switch (type) {
            case 0: // string

                SingleColumnValueFilter filterStr1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER_OR_EQUAL, left);

                SingleColumnValueFilter filterStr2 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS, right);

                filters.add(filterStr1);
                filters.add(filterStr2);
                return filters;
            case 1: // byte
                SingleColumnValueFilter filterB1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER_OR_EQUAL, new ByteComparator(left[0]));

                SingleColumnValueFilter filterB2 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS, new ByteComparator(right[0]));

                filters.add(filterB1);
                filters.add(filterB2);
                return filters;
            case 2: // short
                SingleColumnValueFilter filterS1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER_OR_EQUAL, new ShortComparator(Bytes.toShort(left)));

                SingleColumnValueFilter filterS2 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS, new ShortComparator(Bytes.toShort(right)));

                filters.add(filterS1);
                filters.add(filterS2);
                return filters;
            case 3: // int
                SingleColumnValueFilter filterI1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER_OR_EQUAL, new IntComparator(Bytes.toInt(left)));

                SingleColumnValueFilter filterI2 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS, new IntComparator(Bytes.toInt(right)));

                filters.add(filterI1);
                filters.add(filterI2);
                return filters;
            case 4: // long
                SingleColumnValueFilter filterL1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER_OR_EQUAL, new LongComparator(Bytes.toLong(left)));

                SingleColumnValueFilter filterL2 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS, new LongComparator(Bytes.toLong(right)));

                filters.add(filterL1);
                filters.add(filterL2);
                return filters;
            default:
                return null;
        }
    }

    private static List<Filter> buildFilterBetweenR(Expression expression, TableDescriptor descriptor) {
        String column = expression.getColumn();
        Integer type = descriptor.getTypeOfColumn(column);
        byte[] left = expression.getValue();
        byte[] right = expression.getOptionValue();
        List<Filter> filters = new ArrayList<>();
        switch (type) {
            case 0: // string

                SingleColumnValueFilter filterStr1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER, left);

                SingleColumnValueFilter filterStr2 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS_OR_EQUAL, right);

                filters.add(filterStr1);
                filters.add(filterStr2);
                return filters;
            case 1: // byte
                SingleColumnValueFilter filterB1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER, new ByteComparator(left[0]));

                SingleColumnValueFilter filterB2 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS_OR_EQUAL, new ByteComparator(right[0]));

                filters.add(filterB1);
                filters.add(filterB2);
                return filters;
            case 2: // short
                SingleColumnValueFilter filterS1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER, new ShortComparator(Bytes.toShort(left)));

                SingleColumnValueFilter filterS2 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS_OR_EQUAL, new ShortComparator(Bytes.toShort(right)));

                filters.add(filterS1);
                filters.add(filterS2);
                return filters;
            case 3: // int
                SingleColumnValueFilter filterI1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER, new IntComparator(Bytes.toInt(left)));

                SingleColumnValueFilter filterI2 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS_OR_EQUAL, new IntComparator(Bytes.toInt(right)));

                filters.add(filterI1);
                filters.add(filterI2);
                return filters;
            case 4: // long
                SingleColumnValueFilter filterL1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER, new LongComparator(Bytes.toLong(left)));

                SingleColumnValueFilter filterL2 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS_OR_EQUAL, new LongComparator(Bytes.toLong(right)));

                filters.add(filterL1);
                filters.add(filterL2);
                return filters;
            default:
                return null;
        }
    }

    private static List<Filter> buildFilterBetweenLR(Expression expression, TableDescriptor descriptor) {
        String column = expression.getColumn();
        Integer type = descriptor.getTypeOfColumn(column);
        byte[] left = expression.getValue();
        byte[] right = expression.getOptionValue();
        List<Filter> filters = new ArrayList<>();
        switch (type) {
            case 0: // string

                SingleColumnValueFilter filterStr1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER_OR_EQUAL, left);

                SingleColumnValueFilter filterStr2 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS_OR_EQUAL, right);

                filters.add(filterStr1);
                filters.add(filterStr2);
                return filters;
            case 1: // byte
                SingleColumnValueFilter filterB1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER_OR_EQUAL, new ByteComparator(left[0]));

                SingleColumnValueFilter filterB2 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS_OR_EQUAL, new ByteComparator(right[0]));

                filters.add(filterB1);
                filters.add(filterB2);
                return filters;
            case 2: // short
                SingleColumnValueFilter filterS1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER_OR_EQUAL, new ShortComparator(Bytes.toShort(left)));

                SingleColumnValueFilter filterS2 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS_OR_EQUAL, new ShortComparator(Bytes.toShort(right)));

                filters.add(filterS1);
                filters.add(filterS2);
                return filters;
            case 3: // int
                SingleColumnValueFilter filterI1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER_OR_EQUAL, new IntComparator(Bytes.toInt(left)));

                SingleColumnValueFilter filterI2 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS_OR_EQUAL, new IntComparator(Bytes.toInt(right)));

                filters.add(filterI1);
                filters.add(filterI2);
                return filters;
            case 4: // long
                SingleColumnValueFilter filterL1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER_OR_EQUAL, new LongComparator(Bytes.toLong(left)));

                SingleColumnValueFilter filterL2 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS_OR_EQUAL, new LongComparator(Bytes.toLong(right)));

                filters.add(filterL1);
                filters.add(filterL2);
                return filters;
            default:
                return null;
        }
    }

    private static List<Filter> buildFilterLT(Expression expression, TableDescriptor descriptor) {
        String column = expression.getColumn();
        Integer type = descriptor.getTypeOfColumn(column);
        byte[] left = expression.getValue();
        List<Filter> filters = new ArrayList<>();
        switch (type) {
            case 0: // string

                SingleColumnValueFilter filterStr1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS, left);

                filters.add(filterStr1);
                return filters;
            case 1: // byte
                SingleColumnValueFilter filterB1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS, new ByteComparator(left[0]));


                filters.add(filterB1);
                return filters;
            case 2: // short
                SingleColumnValueFilter filterS1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS, new ShortComparator(Bytes.toShort(left)));

                filters.add(filterS1);
                return filters;
            case 3: // int
                SingleColumnValueFilter filterI1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS, new IntComparator(Bytes.toInt(left)));


                filters.add(filterI1);
                return filters;
            case 4: // long
                SingleColumnValueFilter filterL1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS, new LongComparator(Bytes.toLong(left)));

                filters.add(filterL1);
                return filters;
            default:
                return null;
        }
    }

    private static List<Filter> buildFilterLE(Expression expression, TableDescriptor descriptor) {
        String column = expression.getColumn();
        Integer type = descriptor.getTypeOfColumn(column);
        byte[] left = expression.getValue();
        List<Filter> filters = new ArrayList<>();
        switch (type) {
            case 0: // string

                SingleColumnValueFilter filterStr1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS_OR_EQUAL, left);

                filters.add(filterStr1);
                return filters;
            case 1: // byte
                SingleColumnValueFilter filterB1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS_OR_EQUAL, new ByteComparator(left[0]));


                filters.add(filterB1);
                return filters;
            case 2: // short
                SingleColumnValueFilter filterS1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS_OR_EQUAL, new ShortComparator(Bytes.toShort(left)));

                filters.add(filterS1);
                return filters;
            case 3: // int
                SingleColumnValueFilter filterI1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS_OR_EQUAL, new IntComparator(Bytes.toInt(left)));


                filters.add(filterI1);
                return filters;
            case 4: // long
                SingleColumnValueFilter filterL1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.LESS_OR_EQUAL, new LongComparator(Bytes.toLong(left)));

                filters.add(filterL1);
                return filters;
            default:
                return null;
        }
    }

    private static List<Filter> buildFilterGE(Expression expression, TableDescriptor descriptor) {
        String column = expression.getColumn();
        Integer type = descriptor.getTypeOfColumn(column);
        byte[] left = expression.getValue();
        List<Filter> filters = new ArrayList<>();
        switch (type) {
            case 0: // string

                SingleColumnValueFilter filterStr1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER_OR_EQUAL, left);

                filters.add(filterStr1);
                return filters;
            case 1: // byte
                SingleColumnValueFilter filterB1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER_OR_EQUAL, new ByteComparator(left[0]));


                filters.add(filterB1);
                return filters;
            case 2: // short
                SingleColumnValueFilter filterS1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER_OR_EQUAL, new ShortComparator(Bytes.toShort(left)));

                filters.add(filterS1);
                return filters;
            case 3: // int
                SingleColumnValueFilter filterI1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER_OR_EQUAL, new IntComparator(Bytes.toInt(left)));


                filters.add(filterI1);
                return filters;
            case 4: // long
                SingleColumnValueFilter filterL1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER_OR_EQUAL, new LongComparator(Bytes.toLong(left)));

                filters.add(filterL1);
                return filters;
            default:
                return null;
        }
    }


    private static List<Filter> buildFilterGT(Expression expression, TableDescriptor descriptor) {
        String column = expression.getColumn();
        Integer type = descriptor.getTypeOfColumn(column);
        byte[] left = expression.getValue();
        List<Filter> filters = new ArrayList<>();
        switch (type) {
            case 0: // string

                SingleColumnValueFilter filterStr1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER, left);

                filters.add(filterStr1);
                return filters;
            case 1: // byte
                SingleColumnValueFilter filterB1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER, new ByteComparator(left[0]));


                filters.add(filterB1);
                return filters;
            case 2: // short
                SingleColumnValueFilter filterS1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER, new ShortComparator(Bytes.toShort(left)));

                filters.add(filterS1);
                return filters;
            case 3: // int
                SingleColumnValueFilter filterI1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER, new IntComparator(Bytes.toInt(left)));


                filters.add(filterI1);
                return filters;
            case 4: // long
                SingleColumnValueFilter filterL1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.GREATER, new LongComparator(Bytes.toLong(left)));

                filters.add(filterL1);
                return filters;
            default:
                return null;
        }
    }

    private static List<Filter> buildFilterNEQ(Expression expression, TableDescriptor descriptor) {
        String column = expression.getColumn();
        Integer type = descriptor.getTypeOfColumn(column);
        byte[] left = expression.getValue();
        List<Filter> filters = new ArrayList<>();
        switch (type) {
            case 0: // string

                SingleColumnValueFilter filterStr1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.NOT_EQUAL, left);

                filters.add(filterStr1);
                return filters;
            case 1: // byte
                SingleColumnValueFilter filterB1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.NOT_EQUAL, new ByteComparator(left[0]));


                filters.add(filterB1);
                return filters;
            case 2: // short
                SingleColumnValueFilter filterS1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.NOT_EQUAL, new ShortComparator(Bytes.toShort(left)));

                filters.add(filterS1);
                return filters;
            case 3: // int
                SingleColumnValueFilter filterI1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.NOT_EQUAL, new IntComparator(Bytes.toInt(left)));


                filters.add(filterI1);
                return filters;
            case 4: // long
                SingleColumnValueFilter filterL1 = new SingleColumnValueFilter(ServiceConstants.BYTES_COLUMN_FAMILY,
                        Bytes.toBytes(column), CompareFilter.CompareOp.NOT_EQUAL, new LongComparator(Bytes.toLong(left)));

                filters.add(filterL1);
                return filters;
            default:
                return null;
        }
    }
}
