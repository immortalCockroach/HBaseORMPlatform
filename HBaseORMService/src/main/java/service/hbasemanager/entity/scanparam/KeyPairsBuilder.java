package service.hbasemanager.entity.scanparam;

import com.immortalcockroach.hbaseorm.entity.query.Expression;
import com.immortalcockroach.hbaseorm.param.enums.ColumnTypeEnum;
import service.hbasemanager.entity.tabldesc.TableDescriptor;
import service.utils.ByteArrayUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by immortalCockroach on 10/5/17.
 */
public class KeyPairsBuilder {


    /**
     * 不等于
     *
     * @param param
     * @return
     */
    public static List<KeyPair> buildKeyPairsNEQ(IndexParam param) {
        byte[] prefix = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(), param.getQualifiers().toArray(new
                String[]{}), param.getIndexNum(), false);
        byte[] startKey = prefix;
        byte[] endKey = ByteArrayUtils.getLargeByteArray(prefix);

        List<KeyPair> res = new ArrayList<>();
        res.add(new KeyPair(startKey, endKey));
        return res;
    }


    /**
     * 大于
     *
     * @param param
     * @param descriptor
     * @return
     */
    public static List<KeyPair> buildKeyPairsGT(IndexParam param, TableDescriptor descriptor) {
        Expression expression = param.getExpression();
        String column = expression.getColumn();
        Integer type = descriptor.getTypeOfColumn(column);
        byte[] value = expression.getValue();

        if (ColumnTypeEnum.isStringType(type)) {
            // String大于的情况  endKey为不包含lastValue的greater startKey为包含lastValue的greater
            byte[] endKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                    param.getQualifiers().toArray(new String[]{}), param.getIndexNum(), true));
            param.addOrUpdateLinePrefix(column, value);
            param.addQualifier(column);
            byte[] startKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                    param.getQualifiers().toArray(new String[]{}), param.getIndexNum(), false));
            List<KeyPair> res = new ArrayList<>();
            res.add(new KeyPair(startKey, endKey));
            return res;
        } else {
            // 整型>的情况，需要根据整型的类型和范围来确定
            return ByteArrayUtils.buildRangeWithSingleRangeGT(param, column, type, value);
        }

    }

    public static List<KeyPair> buildKeyPairsGE(IndexParam param, TableDescriptor descriptor) {
        Expression expression = param.getExpression();
        String column = expression.getColumn();
        Integer type = descriptor.getTypeOfColumn(column);
        byte[] value = expression.getValue();

        if (ColumnTypeEnum.isStringType(type)) {

            // String大于等于的情况  endKey为不包含lastValue的greater startKey为包含lastValue
            byte[] endKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                    param.getQualifiers().toArray(new String[]{}), param.getIndexNum(), false));
            param.addOrUpdateLinePrefix(column, value);
            param.addQualifier(column);
            byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                    param.getQualifiers().toArray(new String[]{}), param.getIndexNum(), true);
            List<KeyPair> res = new ArrayList<>();
            res.add(new KeyPair(startKey, endKey));
            return res;
        } else {
            // 整型>=的情况，需要根据整型的类型和范围来确定
            return ByteArrayUtils.buildRangeWithSingleRangeGE(param, column, type, value);
        }

    }

    public static List<KeyPair> buildKeyPairsLT(IndexParam param, TableDescriptor descriptor) {
        Expression expression = param.getExpression();
        String column = expression.getColumn();
        Integer type = descriptor.getTypeOfColumn(column);
        byte[] value = expression.getValue();

        if (ColumnTypeEnum.isStringType(type)) {
            // String小于的情况  startKey为不包含lastValue endKey为包含lastValue
            byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                    param.getQualifiers().toArray(new String[]{}), param.getIndexNum(), false);

            param.addOrUpdateLinePrefix(column, value);
            param.addQualifier(column);
            byte[] endKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                    param.getQualifiers().toArray(new String[]{}), param.getIndexNum(), true);

            List<KeyPair> res = new ArrayList<>();
            res.add(new KeyPair(startKey, endKey));
            return res;
        } else {
            // 整型<的情况，需要根据整型的类型和范围来确定
            return ByteArrayUtils.buildRangeWithSingleRangeLT(param, column, type, value);
        }

    }

    public static List<KeyPair> buildKeyPairsLE(IndexParam param, TableDescriptor descriptor) {
        Expression expression = param.getExpression();
        String column = expression.getColumn();
        Integer type = descriptor.getTypeOfColumn(column);
        byte[] value = expression.getValue();

        if (ColumnTypeEnum.isStringType(type)) {
            // String小于等于的情况  startKey为不包含lastValue endKey为包含lastValue的greater
            byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                    param.getQualifiers().toArray(new String[]{}), param.getIndexNum(), false);

            param.addOrUpdateLinePrefix(column, value);
            param.addQualifier(column);
            byte[] endKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                    param.getQualifiers().toArray(new String[]{}), param.getIndexNum(), true));

            List<KeyPair> res = new ArrayList<>();
            res.add(new KeyPair(startKey, endKey));
            return res;
        } else {
            // 整型<=的情况，需要根据整型的类型和范围来确定
            return ByteArrayUtils.buildRangeWithSingleRangeLE(param, column, type, value);
        }

    }

    public static List<KeyPair> buildKeyPairsBetween(IndexParam param, TableDescriptor descriptor) {
        Expression expression = param.getExpression();
        String column = expression.getColumn();
        Integer type = descriptor.getTypeOfColumn(column);
        byte[] lower = expression.getValue();
        byte[] upper = expression.getOptionValue();

        if (ColumnTypeEnum.isStringType(type)) {
            // String()的情况  startKey为包含lower的greater endKey为包含upper

            param.addOrUpdateLinePrefix(column, lower);
            param.addQualifier(column);
            byte[] startKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                    param.getQualifiers().toArray(new String[]{}), param.getIndexNum(), true));

            param.addOrUpdateLinePrefix(column, upper);
            byte[] endKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                    param.getQualifiers().toArray(new String[]{}), param.getIndexNum(), true);

            List<KeyPair> res = new ArrayList<>();
            res.add(new KeyPair(startKey, endKey));
            return res;
        } else {
            // 整型<=的情况，需要根据整型的类型和范围来确定
            return ByteArrayUtils.buildRangeWithDoubleRangeBetween(param, column, type, lower, upper);
        }

    }

    public static List<KeyPair> buildKeyPairsBetweenL(IndexParam param, TableDescriptor descriptor) {
        Expression expression = param.getExpression();
        String column = expression.getColumn();
        Integer type = descriptor.getTypeOfColumn(column);
        byte[] lower = expression.getValue();
        byte[] upper = expression.getOptionValue();

        if (ColumnTypeEnum.isStringType(type)) {
            // String[)的情况  startKey为包含lower endKey为包含upper

            param.addOrUpdateLinePrefix(column, lower);
            param.addQualifier(column);
            byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                    param.getQualifiers().toArray(new String[]{}), param.getIndexNum(), true);

            param.addOrUpdateLinePrefix(column, upper);
            byte[] endKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                    param.getQualifiers().toArray(new String[]{}), param.getIndexNum(), true);

            List<KeyPair> res = new ArrayList<>();
            res.add(new KeyPair(startKey, endKey));
            return res;
        } else {
            // 整型<=的情况，需要根据整型的类型和范围来确定
            return ByteArrayUtils.buildRangeWithDoubleRangeBetweenL(param, column, type, lower, upper);
        }

    }

    public static List<KeyPair> buildKeyPairsBetweenR(IndexParam param, TableDescriptor descriptor) {
        Expression expression = param.getExpression();
        String column = expression.getColumn();
        Integer type = descriptor.getTypeOfColumn(column);
        byte[] lower = expression.getValue();
        byte[] upper = expression.getOptionValue();

        if (ColumnTypeEnum.isStringType(type)) {
            // String(]的情况  startKey为包含lower endKey为包含upper

            param.addOrUpdateLinePrefix(column, lower);
            param.addQualifier(column);
            byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                    param.getQualifiers().toArray(new String[]{}), param.getIndexNum(), true);

            param.addOrUpdateLinePrefix(column, upper);
            byte[] endKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                    param.getQualifiers().toArray(new String[]{}), param.getIndexNum(), true);

            List<KeyPair> res = new ArrayList<>();
            res.add(new KeyPair(startKey, endKey));
            return res;
        } else {
            // 整型<=的情况，需要根据整型的类型和范围来确定
            return ByteArrayUtils.buildRangeWithDoubleRangeBetweenL(param, column, type, lower, upper);
        }

    }
}
