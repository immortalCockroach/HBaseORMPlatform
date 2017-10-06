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
     * @param descriptor
     * @return
     */
    public static List<KeyPair> buildKeyPairsNEQ(IndexParam param, TableDescriptor descriptor) {
        byte[] prefix = ByteArrayUtils.generateIndexRowKey(param.getLinePrefix(), param.getQualifiers().toArray(new
                String[]{}), param.getIndexNum());
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
            // String大于的情况  endKey为不包含运算符的greater startKey为包含不等运算符的greater
            byte[] endKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.generateIndexRowKey(param.getLinePrefix(),
                    param.getQualifiers().toArray(new String[]{}), param.getIndexNum()));
            param.addOrUpdateLinePrefix(column, value);
            param.addQualifier(column);
            byte[] startKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.generateIndexRowKey(param.getLinePrefix
                    (), param.getQualifiers().toArray(new String[]{}), param.getIndexNum()));
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

            // String大于等于的情况  endKey为不包含运算符的greater startKey为包含不等运算符
            byte[] endKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.generateIndexRowKey(param.getLinePrefix(),
                    param.getQualifiers().toArray(new String[]{}), param.getIndexNum()));
            param.addOrUpdateLinePrefix(column, value);
            param.addQualifier(column);
            byte[] startKey = ByteArrayUtils.generateIndexRowKey(param.getLinePrefix
                    (), param.getQualifiers().toArray(new String[]{}), param.getIndexNum());
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
            // String小于的情况  startKey为不包含运算符 endKey为包含运算符
            byte[] startKey = ByteArrayUtils.generateIndexRowKey(param.getLinePrefix(),
                    param.getQualifiers().toArray(new String[]{}), param.getIndexNum());

            param.addOrUpdateLinePrefix(column, value);
            param.addQualifier(column);
            byte[] endKey = ByteArrayUtils.generateIndexRowKey(param.getLinePrefix(),
                    param.getQualifiers().toArray(new String[]{}), param.getIndexNum());

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
            // String等于的情况  startKey为不包含运算符 endKey为包含运算符的greater
            byte[] startKey = ByteArrayUtils.generateIndexRowKey(param.getLinePrefix(),
                    param.getQualifiers().toArray(new String[]{}), param.getIndexNum());

            param.addOrUpdateLinePrefix(column, value);
            param.addQualifier(column);
            byte[] endKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.generateIndexRowKey(param.getLinePrefix(),
                    param.getQualifiers().toArray(new String[]{}), param.getIndexNum()));

            List<KeyPair> res = new ArrayList<>();
            res.add(new KeyPair(startKey, endKey));
            return res;
        } else {
            // 整型<=的情况，需要根据整型的类型和范围来确定
            return ByteArrayUtils.buildRangeWithSingleRangeLE(param, column, type, value);
        }

    }
}
