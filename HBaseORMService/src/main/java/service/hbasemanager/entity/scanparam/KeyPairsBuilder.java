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
     * 大于
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

        if (ColumnTypeEnum.isStringType(type)) {
            byte[] endKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.generateIndexRowKey(param.getLinePrefix(),
                    param.getQualifiers().toArray(new String[]{}), param.getIndexNum()));

            // String大于的情况  endKey为不包含运算符的greater startKey为包含不等运算符的greater
            byte[] value = expression.getValue();
            param.addLinePrefix(column, value);
            param.addQualifier(column);
            byte[] startKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.generateIndexRowKey(param.getLinePrefix
                    (), param.getQualifiers().toArray(new String[]{}), param.getIndexNum()));
            List<KeyPair> res = new ArrayList<>();
            res.add(new KeyPair(startKey, endKey));
            return res;
        } else {
            // 整型>的情况，需要根据整型的类型和范围来确定

        }

    }
}
