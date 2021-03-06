package service.utils;

import com.alibaba.fastjson.JSONObject;
import com.immortalcockroach.hbaseorm.constant.CommonConstants;
import com.immortalcockroach.hbaseorm.entity.query.Expression;
import com.immortalcockroach.hbaseorm.param.enums.ArithmeticOperatorEnum;
import com.immortalcockroach.hbaseorm.util.Bytes;
import org.apache.commons.lang.ArrayUtils;
import service.constants.ServiceConstants;
import service.hbasemanager.entity.index.Index;
import service.hbasemanager.entity.scanparam.IndexParam;
import service.hbasemanager.entity.scanparam.KeyPair;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 用于索引相关的功能，例如转义、切分等操作
 * Created by immortalCockroach on 8/31/17.
 */
public class ByteArrayUtils {

    /**
     * 将list中每个字节数组中的转义符进行扩充
     *
     * @param list
     * @param separator
     * @return 返回值为转义扩充后的总长度
     */
    @Deprecated
    public static int preProcessEscapeCharacterOfBytes(byte[][] list, byte separator, byte escape) {
        int size = list.length;

        int length = 0;
        for (int i = 0; i <= size - 1; i++) {
            List<Byte> tmp = new ArrayList<>();
            byte[] array = list[i];
            // 加入对NULL字段的支持
            if (array == null) {
                tmp.add(ServiceConstants.NULL);
            }
            if (array.length == 1 && array[0] == ServiceConstants.NULL) {
                tmp.add(escape);
                tmp.add(ServiceConstants.NULL);
            } else {
                for (byte b : array) {
                    // 转义符、分隔符前面加上转义，即ESC->ESC ESC, EOT->ESC EOT
                    if (b == separator || b == escape) {
                        // 填充转义符以及原先的字符
                        tmp.add(escape);
                        tmp.add(b);
                    } else {
                        // 普通字符直接填充
                        tmp.add(b);
                    }
                }
            }

            Byte[] newArray = new Byte[tmp.size()];
            length += newArray.length;
            tmp.toArray(newArray);
            list[i] = ArrayUtils.toPrimitive(newArray);

        }
        return length;
    }


    /**
     * 将byte[]数组进行拼接，用separator隔开
     *
     * @param list
     * @param separator
     * @param length    原先list中的总byte的数量
     * @return
     */
    @Deprecated
    public static byte[] concat(byte[][] list, byte separator, int length, boolean includeLastValue) {
        int size = list.length;
        // length个字节的长度 加上size - 1个分割符，
        // 如果构建索引表扫描前缀不需要最后一个value时，则减掉最后一个value的长度（但是之前的_依旧保留）
        byte[] res = new byte[length + size - 1 - (includeLastValue ? 0 : list[size - 1].length)];
        int index = 0;
        for (int i = 0; i <= size - 2; i++) {
            byte[] col = list[i];

            System.arraycopy(col, 0, res, index, col.length);
            index += col.length;

            res[index] = separator;
            index++;
        }
        // 拷贝最后一个 后面不带分隔符
        if (includeLastValue) {
            System.arraycopy(list[size - 1], 0, res, index, list[size - 1].length);
        }

        return res;

    }


    /**
     * 将byte[]数组按照separator分割
     * 需要跳过被转义的字符
     *
     * @param array
     * @param separator
     * @param escape
     * @return
     */
    @Deprecated
    public static byte[][] splitArrayWithStandAloneSeprator(byte[] array, byte separator, byte escape) {
        List<byte[]> list = new ArrayList<>();
        int size = array.length;

        int index = 0;
        List<Byte> tmp = new ArrayList<>();
        // 用于甄别NUL的
        int lastSep = -1;
        while (index <= size - 1) {
            byte content = array[index];
            if (content == escape) {
                // 说明是转义符，拷贝下一个字节到缓冲区
                index++;
                tmp.add(array[index]);
                index++;
            } else if (content == separator) {
                // 分隔符，直接跳过
                Byte[] newArray = new Byte[tmp.size()];
                tmp.toArray(newArray);
                // NUL的转义需要单独处理（可能是转义的NUL，也可能实际的内容就是NUL）
                if (newArray.length == 1 && newArray[0] == ServiceConstants.NULL) {
                    // 如果差2个位置，说明是_NUL_的形式
                    if (index - lastSep == 2) {
                        newArray = new Byte[0];
                        list.add(ArrayUtils.toPrimitive(newArray));
                    } else {
                        list.add(ArrayUtils.toPrimitive(newArray));
                    }
                } else {
                    list.add(ArrayUtils.toPrimitive(newArray));
                }
                lastSep = index;
                index++;
                // 清空tmp中的内容
                tmp.clear();
            } else {
                tmp.add(array[index]);
                index++;
            }
        }
        // 将最后一段加入（最后一段是rowkey）
        Byte[] newArray = new Byte[tmp.size()];
        tmp.toArray(newArray);
        list.add(ArrayUtils.toPrimitive(newArray));

        byte[][] res = new byte[list.size()][];
        return list.toArray(res);
    }


    /**
     * 将jsonObject的按照qualifiers的顺序组成一个byte[][]
     * 格式为indexNum_col1_col1v_col2_col2v... rowkey
     *
     * @param line
     * @param qualifiers 列修饰符 不包含rowkey
     * @return
     */
    @Deprecated
    public static byte[][] jsonObjectToByteArrayList(JSONObject line, String[] qualifiers, byte indexNum) {
        int size = qualifiers.length;
        // 总长度为qualifies.length * 2  + 2对应 length * 2个col + colv 以及1个rowkey、1个indexNum的值
        byte[][] res = new byte[size * 2 + 2][];
        res[0] = new byte[]{indexNum};
        for (int i = 0; i <= size - 1; i++) {
            res[2 * i + 1] = Bytes.toBytes(qualifiers[i]);
            res[2 * i + 2] = line.getBytes(qualifiers[i]);
        }
        // 最后一个为rowkey的
        res[2 * size + 1] = line.getBytes(CommonConstants.ROW_KEY);

        return res;
    }

    /**
     * 同上面的函数
     *
     * @param line
     * @param qualifiers
     * @param indexNum
     * @return
     */
    @Deprecated
    public static byte[][] mapToByteArray(Map<String, byte[]> line, String[] qualifiers, byte indexNum) {
        int size = qualifiers.length;
        // 总长度为qualifies.length * 2  + 2对应 length * 2个col + colv 以及1个rowkey、1个indexNum的值
        byte[][] res = new byte[size * 2 + 2][];
        res[0] = new byte[]{indexNum};
        for (int i = 0; i <= size - 1; i++) {
            res[2 * i + 1] = Bytes.toBytes(qualifiers[i]);
            res[2 * i + 2] = line.get(qualifiers[i]);
        }
        // 最后一个为rowkey的值
        res[2 * size + 1] = line.get(CommonConstants.ROW_KEY);

        return res;
    }

    /**
     * 索引扫描的构建前缀
     *
     * @param line
     * @param qualifiers
     * @param indexNum
     * @return
     */
    @Deprecated
    public static byte[][] mapToByteArrayWithOutRowKey(Map<String, byte[]> line, String[] qualifiers, byte indexNum) {
        int size = qualifiers.length;
        // 总长度为qualifies.length * 2  + 1对应 length * 2个col + colv 以及1个indexNum的值
        byte[][] res = new byte[size * 2 + 1][];
        res[0] = new byte[]{indexNum};
        for (int i = 0; i <= size - 1; i++) {
            res[2 * i + 1] = Bytes.toBytes(qualifiers[i]);
            res[2 * i + 2] = line.get(qualifiers[i]);
        }

        return res;
    }


    /**
     * 更新或者创建索引时构建索引信息
     * 1、转义
     * 2、拼接
     *
     * @param line
     * @return
     */
    public static byte[] generateIndexRowKey(JSONObject line, Index index) {
        byte[] rowkey = line.getBytes(CommonConstants.ROW_KEY);
        int length = calLength(index, rowkey);
        ByteArrayBuilder builder = new ByteArrayBuilder(length);

        builder.put(index.getIndexNum());

        String[] qualifiers = index.getIndexColumnList();
        int[] sizeOfIndexes = index.getSize();
        int size = qualifiers.length;
        for (int i = 0; i <= size - 1; i++) {
            String qualifier = qualifiers[i];
            byte[] content = line.getBytes(qualifier);
            int sizeOfIndex = sizeOfIndexes[i];
            if (ArrayUtils.isEmpty(content)) {

                builder.put((byte) 0);
                builder.addPosition(sizeOfIndex);
            } else {
                builder.put((byte) 1);
                builder.put(fixSize(content, sizeOfIndex));
            }
        }

        builder.put(rowkey);
        return builder.array();
    }


    /**
     * 插入数据时更新索引表信息
     *
     * @param line
     * @return
     */
    public static byte[] generateIndexRowKey(Map<String, byte[]> line, Index index) {
        byte[] rowkey = line.get(CommonConstants.ROW_KEY);
        int length = calLength(index, rowkey);
        ByteArrayBuilder builder = new ByteArrayBuilder(length);

        builder.put(index.getIndexNum());

        String[] qualifiers = index.getIndexColumnList();
        int[] sizeOfIndexes = index.getSize();
        int size = qualifiers.length;
        for (int i = 0; i <= size - 1; i++) {
            String qualifier = qualifiers[i];
            byte[] content = line.get(qualifier);
            int sizeOfIndex = sizeOfIndexes[i];
            if (ArrayUtils.isEmpty(content)) {

                builder.put((byte) 0);
                builder.addPosition(sizeOfIndex);
            } else {
                builder.put((byte) 1);
                builder.put(fixSize(content, sizeOfIndex));
            }
        }

        builder.put(rowkey);
        return builder.array();
    }


    /**
     * 构建索引表查询时前缀的方式
     *
     * @param line
     * @param includeLastValue 代表最后的列值是否需要加入
     * @return
     */
    public static byte[] buildIndexTableScanPrefix(Map<String, byte[]> line, int hitNum, Index index,
                                                   boolean includeLastValue) {
        int length = calLength(index, hitNum, includeLastValue);
        ByteArrayBuilder builder = new ByteArrayBuilder(length);

        builder.put(index.getIndexNum());

        String[] qualifiers = index.getIndexColumnList();
        int[] sizeOfIndexes = index.getSize();
        int i;
        for (i = 0; i <= hitNum - 2; i++) {
            String qualifier = qualifiers[i];
            byte[] content = line.get(qualifier);
            int sizeOfIndex = sizeOfIndexes[i];
            if (ArrayUtils.isEmpty(content)) {

                builder.put((byte) 0);
                builder.addPosition(sizeOfIndex);
            } else {
                builder.put((byte) 1);
                builder.put(fixSize(content, sizeOfIndex));
            }
        }
        builder.put((byte) 1);
        if (includeLastValue) {
            String qualifier = qualifiers[i];
            byte[] content = line.get(qualifier);
            builder.put(fixSize(content, sizeOfIndexes[i]));
        }


        return builder.array();
    }

    /**
     * 根据key的要求进行加长或者截断
     * @param key
     * @param maxLength
     * @return
     */
    private static byte[] fixSize(byte[] key, int maxLength) {
        if (maxLength == key.length) {
            return key;
        } else if (maxLength < key.length) {
            byte[] res = new byte[maxLength];
            System.arraycopy(key, 0, res, 0, maxLength);
            return res;
        } else {
            byte[] res = new byte[maxLength];
            System.arraycopy(key, 0, res, 0, key.length);
            return res;
        }

    }

    /**
     * 计算索引行键的长度
     * indexNum + indexSize + (0/1)paddiing * size + rowkeylength
     *
     * @param index
     * @param rowkey
     * @return
     */
    private static int calLength(Index index, byte[] rowkey) {
        return 1 + index.getIndexLength() + index.getSize().length + rowkey.length;
    }

    /**
     * 基于索引查询时的行键构建
     *
     * @param index
     * @param hitSize
     * @return
     */
    private static int calLength(Index index, int hitSize, boolean includeLastValue) {
        int length = 1;
        int i;
        int[] size = index.getSize();
        for (i = 0; i <= hitSize - 2; i++) {
            length += (1 + size[i]);
        }
        length += 1;
        if (includeLastValue) {
            length += size[i];
        }
        return length;
    }

    /**
     * 根据Separator将字节数组进行分割
     * <p>
     * <p>
     * 分割后的格式为indexNum_col1_col1v_col2_col2v...rowkey
     *
     * @param array
     * @return
     */
    public static byte[][] getByteListWithSeparator(byte[] array, Index index) {
        String[] qualifiers = index.getIndexColumnList();
        int[] sizeOfIndexes = index.getSize();
        // 长度为indexNum + rowkey + 2 * length(KV)
        int length = 2 + 2 * qualifiers.length;
        byte[][] res = new byte[length][];

        res[0] = new byte[]{array[0]};
        int rawIndex = 1;
        for (int i = 0; i <= qualifiers.length - 1; i++) {
            res[2 * i + 1] = Bytes.toBytes(qualifiers[i]);
            byte flag = array[rawIndex];
            rawIndex++;
            int sizeofIndex = sizeOfIndexes[i];
            res[2 * i + 2] = new byte[sizeofIndex];
            if (flag == 1) {
                System.arraycopy(array, rawIndex, res[2 * i + 2], 0, sizeofIndex);
            }
            rawIndex += sizeofIndex;
        }
        // copy rowkey
        res[length - 1] = new byte[array.length - rawIndex];
        System.arraycopy(array, rawIndex, res[length - 1], 0, array.length - rawIndex);

        return res;
    }

    public static byte[] getIndexTableName(byte[] tableName) {
        return Bytes.toBytes(Bytes.toString(tableName) + ServiceConstants.INDEX_SUFFIX);

    }

    /**
     * 根据表达式和value进行检查
     *
     * @return
     */
    public static boolean checkValueRange(byte[] value, Expression expression, Integer columnType) {
        byte[] expressionValue = expression.getValue();
        Integer operatorId = expression.getArithmeticOperator();
        // 空值的情况下，必须是EQ或者是NEQ才行
        if (ArrayUtils.isEmpty(value) || ArrayUtils.isEmpty(expressionValue)) {
            if (operatorId == ArithmeticOperatorEnum.EQ.getId()) {
                if (ArrayUtils.isEmpty(expressionValue) && ArrayUtils.isEmpty(value)) {
                    return true;
                } else {
                    return false;
                }
            } else if (operatorId == ArithmeticOperatorEnum.NEQ.getId()) {
                // 如果非2个都null 则为true
                if (!(ArrayUtils.isEmpty(expressionValue) && ArrayUtils.isEmpty(value))) {
                    return true;
                } else {
                    return false;
                }
            } else {
                // 有null 但是是范围查询，则一定错误
                return false;
            }
        }
        switch (columnType) {
            case 0:
                return checkString(Bytes.toString(fixString(value)), expression);
            case 1:
                return checkByte(value[0], expression);
            case 2:
                return checkShort(Bytes.toShort(value), expression);
            case 3:
                return checkInt(Bytes.toInt(value), expression);
            case 4:
                return checkLong(Bytes.toLong(value), expression);
            default:
                return false;
        }
    }

    /**
     * 去掉字符串后的padding值
     * @param value
     * @return
     */
    public static byte[] fixString(byte[] value) {
        int index = value.length - 1;
        while (index >= 0 && value[index] == 0) {
            index--;
        }
        if (index == -1) {
            return new byte[0];
        } else {
            byte[] res = new byte[index + 1];
            System.arraycopy(value, 0, res, 0, index + 1);
            return res;
        }
    }

    public static byte[][] getRowkeys(Set<ByteBuffer> rowkeys) {
        int size = rowkeys.size();
        byte[][] res = new byte[size][];
        int index = 0;
        for (ByteBuffer b : rowkeys) {
            res[index] = b.array();
            index++;
        }
        return res;
    }

    public static boolean checkString(String s, Expression expression) {
        Integer operatorId = expression.getArithmeticOperator();
        // l为表达式中的值
        String l = Bytes.toString(expression.getValue());
        if (ArithmeticOperatorEnum.isDoubleRange(operatorId)) {
            String r = Bytes.toString(expression.getOptionValue());
            if (operatorId == ArithmeticOperatorEnum.BETWEEN.getId()) {
                return s.compareTo(l) > 0 && s.compareTo(r) < 0;
            } else if (operatorId == ArithmeticOperatorEnum.BETWEENL.getId()) {
                return s.compareTo(l) >= 0 && s.compareTo(r) < 0;
            } else if (operatorId == ArithmeticOperatorEnum.BETWEENR.getId()) {
                return s.compareTo(l) > 0 && s.compareTo(r) <= 0;
            } else {
                return s.compareTo(l) >= 0 && s.compareTo(r) <= 0;
            }
        } else if (ArithmeticOperatorEnum.isSingleRange(operatorId)) { //
            if (operatorId == ArithmeticOperatorEnum.GT.getId()) {
                return s.compareTo(l) > 0;
            } else if (operatorId == ArithmeticOperatorEnum.GE.getId()) {
                return s.compareTo(l) >= 0;
            } else if (operatorId == ArithmeticOperatorEnum.LT.getId()) {
                return s.compareTo(l) < 0;
            } else {
                return s.compareTo(l) <= 0;
            }
        } else {
            // 不等于的情况下必须扫索引表的该前缀的全部，后面的再过滤
            if (operatorId == ArithmeticOperatorEnum.EQ.getId()) {
                return s.compareTo(l) == 0;
            } else {
                return s.compareTo(l) != 0;
            }

        }
    }

    public static boolean checkByte(byte b, Expression expression) {
        Integer operatorId = expression.getArithmeticOperator();
        // l为表达式中的值
        byte l = expression.getValue()[0];
        if (ArithmeticOperatorEnum.isDoubleRange(operatorId)) {
            byte r = expression.getOptionValue()[0];
            if (operatorId == ArithmeticOperatorEnum.BETWEEN.getId()) {
                return b > l && b < r;
            } else if (operatorId == ArithmeticOperatorEnum.BETWEENL.getId()) {
                return b >= l && b < r;
            } else if (operatorId == ArithmeticOperatorEnum.BETWEENR.getId()) {
                return b > l && b <= r;
            } else {
                return b >= l && b <= r;
            }
        } else if (ArithmeticOperatorEnum.isSingleRange(operatorId)) { //
            if (operatorId == ArithmeticOperatorEnum.GT.getId()) {
                return b > l;
            } else if (operatorId == ArithmeticOperatorEnum.GE.getId()) {
                return b >= l;
            } else if (operatorId == ArithmeticOperatorEnum.LT.getId()) {
                return b < l;
            } else {
                return b <= l;
            }
        } else {
            // 不等于的情况下必须扫索引表的该前缀的全部，后面的再过滤
            if (operatorId == ArithmeticOperatorEnum.EQ.getId()) {
                return b == l;
            } else {
                return b != l;
            }
        }
    }

    public static boolean checkShort(short s, Expression expression) {
        Integer operatorId = expression.getArithmeticOperator();
        // l为表达式中的值
        short l = Bytes.toShort(expression.getValue());
        if (ArithmeticOperatorEnum.isDoubleRange(operatorId)) {
            short r = Bytes.toShort(expression.getOptionValue());
            if (operatorId == ArithmeticOperatorEnum.BETWEEN.getId()) {
                return s > l && s < r;
            } else if (operatorId == ArithmeticOperatorEnum.BETWEENL.getId()) {
                return s >= l && s < r;
            } else if (operatorId == ArithmeticOperatorEnum.BETWEENR.getId()) {
                return s > l && s <= r;
            } else {
                return s >= l && s <= r;
            }
        } else if (ArithmeticOperatorEnum.isSingleRange(operatorId)) { //
            if (operatorId == ArithmeticOperatorEnum.GT.getId()) {
                return s > l;
            } else if (operatorId == ArithmeticOperatorEnum.GE.getId()) {
                return s >= l;
            } else if (operatorId == ArithmeticOperatorEnum.LT.getId()) {
                return s < l;
            } else {
                return s <= l;
            }
        } else {
            // 不等于的情况下必须扫索引表的该前缀的全部，后面的再过滤
            if (operatorId == ArithmeticOperatorEnum.EQ.getId()) {
                return s == l;
            } else {
                return s != l;
            }
        }
    }

    public static boolean checkInt(int i, Expression expression) {
        Integer operatorId = expression.getArithmeticOperator();
        // l为表达式中的值
        int l = Bytes.toInt(expression.getValue());
        if (ArithmeticOperatorEnum.isDoubleRange(operatorId)) {
            int r = Bytes.toInt(expression.getOptionValue());
            if (operatorId == ArithmeticOperatorEnum.BETWEEN.getId()) {
                return i > l && i < r;
            } else if (operatorId == ArithmeticOperatorEnum.BETWEENL.getId()) {
                return i >= l && i < r;
            } else if (operatorId == ArithmeticOperatorEnum.BETWEENR.getId()) {
                return i > l && i <= r;
            } else {
                return i >= l && i <= r;
            }
        } else if (ArithmeticOperatorEnum.isSingleRange(operatorId)) { //
            if (operatorId == ArithmeticOperatorEnum.GT.getId()) {
                return i > l;
            } else if (operatorId == ArithmeticOperatorEnum.GE.getId()) {
                return i >= l;
            } else if (operatorId == ArithmeticOperatorEnum.LT.getId()) {
                return i < l;
            } else {
                return i <= l;
            }
        } else {
            // 不等于的情况下必须扫索引表的该前缀的全部，后面的再过滤
            if (operatorId == ArithmeticOperatorEnum.EQ.getId()) {
                return i == l;
            } else {
                return i != l;
            }
        }
    }

    public static boolean checkLong(long l, Expression expression) {
        Integer operatorId = expression.getArithmeticOperator();
        // l为表达式中的值
        int L = Bytes.toInt(expression.getValue());
        if (ArithmeticOperatorEnum.isDoubleRange(operatorId)) {
            long R = Bytes.toLong(expression.getOptionValue());
            if (operatorId == ArithmeticOperatorEnum.BETWEEN.getId()) {
                return l > L && l < R;
            } else if (operatorId == ArithmeticOperatorEnum.BETWEENL.getId()) {
                return l > L && l < R;
            } else if (operatorId == ArithmeticOperatorEnum.BETWEENR.getId()) {
                return l > L && l < R;
            } else {
                return l > L && l < R;
            }
        } else if (ArithmeticOperatorEnum.isSingleRange(operatorId)) { //
            if (operatorId == ArithmeticOperatorEnum.GT.getId()) {
                return l > L;
            } else if (operatorId == ArithmeticOperatorEnum.GE.getId()) {
                return l >= L;
            } else if (operatorId == ArithmeticOperatorEnum.LT.getId()) {
                return l < L;
            } else {
                return l <= L;
            }
        } else {
            if (operatorId == ArithmeticOperatorEnum.EQ.getId()) {
                return L == l;
            } else {
                return L != l;
            }
        }
    }

    /**
     * 得到字典序大1的
     * 此处的字节字典序大小为 0 1 ... 127 -128 .... -1
     * 此处全1的情况不会出现
     *
     * @param origin
     * @return
     */
    public static byte[] getLargeByteArray(byte[] origin) {
        int size = origin.length;
        byte[] res = new byte[origin.length];
        System.arraycopy(origin, 0, res, 0, origin.length);
        for (int i = size - 1; i >= 0; i--) {
            if (res[i] != -1) {
                res[i]++;
                break;
            } else {
                // 进位
                res[i] = 0;
            }
        }
        return res;
    }

    /**
     * 将一个数组置为字典序最大
     *
     * @param array
     */
    public static void fillBytes(byte[] array) {
        if (array == null || array.length == 0) {
            return;
        }
        Arrays.fill(array, (byte) 1);
    }

    /**
     * 置为字典序的最小
     *
     * @param array
     */
    public static void resetBytes(byte[] array) {
        if (array == null || array.length == 0) {
            return;
        }
        Arrays.fill(array, (byte) 0);
    }

    /**
     * @param param
     * @param column
     * @param type
     * @return
     */
    public static List<KeyPair> buildRangeWithSingleRangeGT(IndexParam param, String column, Integer type, byte[] value) {
        switch (type) {
            case 1: // byte
                byte b = value[0];
                if (b >= Byte.MAX_VALUE) {
                    return null;
                }
                if (b >= (byte) -1) { // checked
                    // startKey为value + 1, endKey为max的greater
                    b++; // 此处可以b++ 因为max的情况被排除了
                    param.addOrUpdateLinePrefix(column, new byte[]{b});

                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, new byte[]{Byte.MAX_VALUE});
                    byte[] endKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else { //  b <= -2
                    // 双端 [0, L(MAX)] & (b, L(-1)]
                    param.addOrUpdateLinePrefix(column, new byte[]{0});

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, new byte[]{Byte.MAX_VALUE});
                    byte[] endKey1 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    b++;
                    param.addOrUpdateLinePrefix(column, new byte[]{b});
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(), param.getHitNum(),
                            param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, new byte[]{(byte) -1});
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }

            case 2: // short
                short s = Bytes.toShort(value);
                if (s >= Short.MAX_VALUE) {
                    return null;
                }
                if (s >= (short) -1) { // checked
                    // startKey为value + 1, endKey为max的greater
                    s++;
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(s));

                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(Short.MAX_VALUE));
                    byte[] endKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else { // checked s <= -2
                    // 双端 [0, L(MAX)] & (s, L(-1)]
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes((short) 0));

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(Short.MAX_VALUE));
                    byte[] endKey1 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    s++;
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(s));
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes((short) -1));
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }

            case 3: // int
                int i = Bytes.toInt(value);
                if (i >= Integer.MAX_VALUE) {
                    return null;
                }
                if (i >= -1) { // checked
                    // startKey为value + 1, endKey为max的greater
                    i++;
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(i));

                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(Integer.MAX_VALUE));
                    byte[] endKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else { // checked i <= -2
                    // 双端 [0, L(MAX)] & (i, L(-1)]
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(0));

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(Integer.MAX_VALUE));
                    byte[] endKey1 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    i++;
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(i));
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(-1));
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }

            case 4: // long
                long l = Bytes.toLong(value);
                if (l >= Long.MAX_VALUE) {
                    return null;
                }
                if (l >= -1L) { // checked
                    // startKey为value + 1, endKey为max的greater
                    l++;
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(l));

                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(Long.MAX_VALUE));
                    byte[] endKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else { // checked
                    // 双端 [0, L(MAX)] & [l + 1, L(-1)]
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(0L));

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(Long.MAX_VALUE));
                    byte[] endKey1 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    l++;
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(l));
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(-1L));
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }
            default:
                return null;
        }
    }

    public static List<KeyPair> buildRangeWithSingleRangeGE(IndexParam param, String column, Integer type, byte[] value) {
        switch (type) {
            case 1: // byte
                byte b = value[0];
                if (b > Byte.MAX_VALUE) {
                    return null;
                }
                if (b >= (byte) 0) {
                    // [b, L(max)]
                    param.addOrUpdateLinePrefix(column, new byte[]{b});

                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, new byte[]{Byte.MAX_VALUE});
                    byte[] endKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix
                            (), param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else {
                    // 双端 [0, L(MAX)] & [b, L(-1)]
                    param.addOrUpdateLinePrefix(column, new byte[]{0});

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, new byte[]{Byte.MAX_VALUE});
                    byte[] endKey1 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix
                            (), param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    param.addOrUpdateLinePrefix(column, new byte[]{b});
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(), param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, new byte[]{(byte) -1});
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }

            case 2: // short
                short s = Bytes.toShort(value);
                if (s > Short.MAX_VALUE) {
                    return null;
                }
                if (s >= (short) 0) {
                    // [s, L(max)]
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(s));

                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(Short.MAX_VALUE));
                    byte[] endKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else {
                    // 双端 [0, L(MAX)] & [s, L(-1)]
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes((short) 0));

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(Short.MAX_VALUE));
                    byte[] endKey1 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(s));
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes((short) -1));
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }

            case 3: // int
                int i = Bytes.toInt(value);
                if (i > Integer.MAX_VALUE) {
                    return null;
                }
                if (i >= 0) {
                    // startKey为value, endKey为max的greater
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(i));

                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(Integer.MAX_VALUE));
                    byte[] endKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else {
                    // 双端 [0, L(MAX)] & [i, L(-1)]
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(0));

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(Integer.MAX_VALUE));
                    byte[] endKey1 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(i));
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(-1));
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }

            case 4: // long
                long l = Bytes.toLong(value);
                if (l > Long.MAX_VALUE) {
                    return null;
                }
                if (l >= 0L) {
                    // [l, L(max)]
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(l));

                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(Long.MAX_VALUE));
                    byte[] endKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else {
                    // 双端 [0, L(MAX)] & [l, L(-1)]
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(0L));

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(Long.MAX_VALUE));
                    byte[] endKey1 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(l));
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(-1L));
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }
            default:
                return null;
        }
    }

    public static List<KeyPair> buildRangeWithSingleRangeLT(IndexParam param, String column, Integer type, byte[] value) {
        switch (type) {
            case 1: // byte
                byte b = value[0];
                if (b <= Byte.MIN_VALUE) {
                    return null;
                }
                if (b <= (byte) 0) {
                    // [min, b)
                    param.addOrUpdateLinePrefix(column, new byte[]{b});

                    byte[] endKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, new byte[]{Byte.MIN_VALUE});
                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else {
                    // 双端 [MIN, -1] & [0, b)
                    param.addOrUpdateLinePrefix(column, new byte[]{0});

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, new byte[]{b});
                    byte[] endKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    param.addOrUpdateLinePrefix(column, new byte[]{Byte.MIN_VALUE});
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(), param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, new byte[]{-1});
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }

            case 2: // short
                short s = Bytes.toShort(value);
                if (s <= Short.MIN_VALUE) {
                    return null;
                }
                if (s <= (short) 0) {
                    // [min, s)
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(s));
                    byte[] endKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(Short.MIN_VALUE));
                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else {
                    // 双端 [MIN, -1] & [0, S)
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes((short) 0));

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(s));
                    byte[] endKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(Short.MIN_VALUE));
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes((short) -1));
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }

            case 3: // int
                int i = Bytes.toInt(value);
                if (i <= Integer.MIN_VALUE) {
                    return null;
                }
                if (i <= 0) {
                    // [min, i)
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(i));

                    byte[] endKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(Integer.MIN_VALUE));
                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else {
                    // 双端 [MIN, -1] & [0, i)
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(0));

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(i));
                    byte[] endKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(Integer.MIN_VALUE));
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(-1));
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }

            case 4: // long
                long l = Bytes.toLong(value);
                if (l <= Long.MIN_VALUE) {
                    return null;
                }
                if (l <= 0L) {
                    // [min, l)
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(l));

                    byte[] endKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(Long.MIN_VALUE));
                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else {
                    //  双端 [MIN, -1] & [0, l)
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(0L));

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(l));
                    byte[] endKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(Long.MIN_VALUE));
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(-1L));
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }
            default:
                return null;
        }
    }

    public static List<KeyPair> buildRangeWithSingleRangeLE(IndexParam param, String column, Integer type, byte[] value) {
        switch (type) {
            case 1: // byte
                byte b = value[0];
                if (b < Byte.MIN_VALUE) {
                    return null;
                }
                if (b < (byte) 0) {
                    // [min, b]
                    b++;
                    param.addOrUpdateLinePrefix(column, new byte[]{b});

                    byte[] endKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, new byte[]{Byte.MIN_VALUE});
                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else { // b >= 0
                    // 双端 [MIN, -1] & [0, b]
                    param.addOrUpdateLinePrefix(column, new byte[]{0});
                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    // b++; 此处不能用b++, 如果b是MAX则越界 下面的类型同理
                    param.addOrUpdateLinePrefix(column, new byte[]{b});
                    byte[] endKey1 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));

                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    param.addOrUpdateLinePrefix(column, new byte[]{Byte.MIN_VALUE});
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(), param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, new byte[]{(byte) -1});
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }

            case 2: // short
                short s = Bytes.toShort(value);
                if (s < Short.MIN_VALUE) {
                    return null;
                }
                if (s < (short) 0) {
                    // [min, s]
                    s++;
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(s));

                    byte[] endKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(Short.MIN_VALUE));
                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else {
                    // 双端 [MIN, -1] & [0, S]
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes((short) 0));

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    // s++;
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(s));
                    byte[] endKey1 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(Short.MIN_VALUE));
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes((short) -1));
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }

            case 3: // int
                int i = Bytes.toInt(value);
                if (i < Integer.MIN_VALUE) {
                    return null;
                }
                if (i < 0) {
                    // [min, i]
                    i++;
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(i));

                    byte[] endKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(Integer.MIN_VALUE));
                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else {
                    // // 双端 [MIN, -1] & [0, i]
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(0));

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    // i++;
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(i));
                    byte[] endKey1 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(Integer.MIN_VALUE));
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(-1));
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }

            case 4: // long
                long l = Bytes.toLong(value);
                if (l < Long.MIN_VALUE) {
                    return null;
                }
                if (l < 0L) {
                    // [min, l]
                    l++;
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(l));


                    byte[] endKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(Long.MIN_VALUE));
                    byte[] startKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix
                            (), param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else {
                    //  双端 [MIN, -1] & [0, l]
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(0L));


                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    //l++;
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(l));
                    byte[] endKey1 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(Long.MIN_VALUE));
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(-1L));
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }
            default:
                return null;
        }
    }

    public static List<KeyPair> buildRangeWithDoubleRangeBetween(IndexParam param, String column, Integer type,
                                                                 byte[] lower, byte[] upper) {
        switch (type) {
            case 1: // byte
                byte l = lower[0];
                byte r = upper[0];
                if (l >= Byte.MAX_VALUE || r <= Byte.MIN_VALUE) {
                    return null;
                }
                if (r <= (byte) 0 || l >= (byte) -1) {
                    // (l, r)
                    l++;
                    param.addOrUpdateLinePrefix(column, new byte[]{l});

                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, new byte[]{r});
                    byte[] endKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else {
                    // 双端 (l, -1] & [0, r)
                    param.addOrUpdateLinePrefix(column, new byte[]{0});

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, new byte[]{r});
                    byte[] endKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    l++;
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    param.addOrUpdateLinePrefix(column, new byte[]{l});
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(), param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, new byte[]{(byte) -1});
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }

            case 2: // short
                short ls = Bytes.toShort(lower);
                short rs = Bytes.toShort(upper);
                if (ls >= Short.MAX_VALUE || rs <= Short.MIN_VALUE) {
                    return null;
                }
                if (rs <= (short) 0 || ls >= (short) -1) {
                    // (ls, rs)
                    ls++;
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(ls));

                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(rs));
                    byte[] endKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else {
                    // 双端 (ls, -1] & [0, rs)
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes((short) 0));

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(rs));
                    byte[] endKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    ls++;
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(ls));
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes((short) -1));
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }

            case 3: // int
                int li = Bytes.toInt(lower);
                int ri = Bytes.toInt(upper);
                if (li >= Integer.MAX_VALUE || ri <= Integer.MIN_VALUE) {
                    return null;
                }
                if (ri <= 0 || li >= -1) {
                    // (li, ri)
                    li++;
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(li));

                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(ri));
                    byte[] endKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else {
                    // 双端 (li, -1] & [0, ri)
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(0));

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(ri));
                    byte[] endKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    li++;
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(li));
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(-1));
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }

            case 4: // long
                long ll = Bytes.toLong(lower);
                long rl = Bytes.toLong(upper);
                if (ll >= Long.MAX_VALUE || rl <= Long.MIN_VALUE) {
                    return null;
                }
                if (rl <= 0L || ll >= -1L) {
                    // (ll, rl)
                    ll++;
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(ll));

                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(rl));
                    byte[] endKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else {
                    // 双端 (ll, -1] & [0, rl)
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(0L));

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(rl));
                    byte[] endKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    ll++;
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(ll));
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(-1L));
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }
            default:
                return null;
        }
    }

    public static List<KeyPair> buildRangeWithDoubleRangeBetweenL(IndexParam param, String column, Integer type,
                                                                  byte[] lower, byte[] upper) {
        switch (type) {
            case 1: // byte
                byte l = lower[0];
                byte r = upper[0];
                if (l > Byte.MAX_VALUE || r <= Byte.MIN_VALUE) {
                    return null;
                }
                if (r <= (byte) 0 || l >= (byte) 0) {
                    // [l, r)

                    param.addOrUpdateLinePrefix(column, new byte[]{l});

                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, new byte[]{r});
                    byte[] endKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else { // r > 0 && l < 0
                    // 双端 [l, -1] & [0, r)
                    param.addOrUpdateLinePrefix(column, new byte[]{0});

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, new byte[]{r});
                    byte[] endKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    param.addOrUpdateLinePrefix(column, new byte[]{l});
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(), param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, new byte[]{(byte) -1});
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }

            case 2: // short
                short ls = Bytes.toShort(lower);
                short rs = Bytes.toShort(upper);
                if (ls > Short.MAX_VALUE || rs <= Short.MIN_VALUE) {
                    return null;
                }
                if (rs <= (short) 0 || ls >= (short) 0) {
                    // [ls, rs)
                    // ls++;
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(ls));

                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(rs));
                    byte[] endKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else {
                    // 双端 [ls, -1] & [0, rs)
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes((short) 0));


                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(rs));
                    byte[] endKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(ls));
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes((short) -1));
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }

            case 3: // int
                int li = Bytes.toInt(lower);
                int ri = Bytes.toInt(upper);
                if (li > Integer.MAX_VALUE || ri <= Integer.MIN_VALUE) {
                    return null;
                }
                if (ri <= 0 || li >= 0) {
                    // [li, ri)
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(li));

                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(ri));
                    byte[] endKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else {
                    // 双端 [li, -1] & [0, ri)
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(0));


                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(ri));
                    byte[] endKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(li));
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(-1));
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }

            case 4: // long
                long ll = Bytes.toLong(lower);
                long rl = Bytes.toLong(upper);
                if (ll > Long.MAX_VALUE || rl <= Long.MIN_VALUE) {
                    return null;
                }
                if (rl <= 0L || ll >= 0L) {
                    // [ll, rl)
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(ll));

                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(rl));
                    byte[] endKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else {
                    // 双端 [ll, -1] & [0, rl)
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(0L));

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(rl));
                    byte[] endKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(ll));
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(-1L));
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }
            default:
                return null;
        }
    }

    public static List<KeyPair> buildRangeWithDoubleRangeBetweenR(IndexParam param, String column, Integer type,
                                                                  byte[] lower, byte[] upper) {
        switch (type) {
            case 1: // byte
                byte l = lower[0];
                byte r = upper[0];
                if (l >= Byte.MAX_VALUE || r < Byte.MIN_VALUE) {
                    return null;
                }
                if (r <= (byte) -1 || l >= (byte) -1) {
                    // (l, r]
                    l++;
                    param.addOrUpdateLinePrefix(column, new byte[]{l});

                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    // r++; 此处不能r++ 可能越界
                    param.addOrUpdateLinePrefix(column, new byte[]{r});
                    byte[] endKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else { // r >= 0 && l <= -2
                    // 双端 (l, -1] & [0, r]
                    param.addOrUpdateLinePrefix(column, new byte[]{0});

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    // 此处r可能是MAX，所以不能++
                    param.addOrUpdateLinePrefix(column, new byte[]{r});
                    byte[] endKey1 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));

                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    l++;
                    param.addOrUpdateLinePrefix(column, new byte[]{l});
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(), param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, new byte[]{(byte) -1});
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }

            case 2: // short
                short ls = Bytes.toShort(lower);
                short rs = Bytes.toShort(upper);
                if (ls >= Short.MAX_VALUE || rs < Short.MIN_VALUE) {
                    return null;
                }
                if (rs <= (short) -1 || ls >= (short) -1) {
                    // (ls, rs]
                    // ls++;
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(ls));

                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(rs));
                    byte[] endKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else { // rs >= 0 && ls <= -2
                    // 双端 (ls, -1] & [0, rs]
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes((short) 0));

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(rs));
                    byte[] endKey1 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    ls++;
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(ls));
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes((short) -1));
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }

            case 3: // int
                int li = Bytes.toInt(lower);
                int ri = Bytes.toInt(upper);
                if (li >= Integer.MAX_VALUE || ri < Integer.MIN_VALUE) {
                    return null;
                }
                if (ri <= -1 || li >= -1) {
                    // [li, ri)
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(li));

                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(ri));
                    byte[] endKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else { // ri >= 0 && lr <= -2
                    // 双端 [li, -1] & [0, ri)
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(0));

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(ri));
                    byte[] endKey1 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    li++;
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(li));
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(-1));
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }

            case 4: // long
                long ll = Bytes.toLong(lower);
                long rl = Bytes.toLong(upper);
                if (ll >= Long.MAX_VALUE || rl < Long.MIN_VALUE) {
                    return null;
                }
                if (rl <= -1L || ll >= -1L) {
                    // (ll, rl)
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(ll));

                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(rl));
                    byte[] endKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else { // rl >= 0 && ll <= -2
                    // 双端 (ll, -1] & [0, rl]
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(0L));

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(rl));
                    byte[] endKey1 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    ll++;
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(ll));
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(-1L));
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }
            default:
                return null;
        }
    }

    public static List<KeyPair> buildRangeWithDoubleRangeBetweenLR(IndexParam param, String column, Integer type,
                                                                   byte[] lower, byte[] upper) {
        switch (type) {
            case 1: // byte
                byte l = lower[0];
                byte r = upper[0];
                if (l > Byte.MAX_VALUE || r < Byte.MIN_VALUE) {
                    return null;
                }
                if (r <= (byte) -1 || l >= (byte) 0) {
                    // [l, r]

                    param.addOrUpdateLinePrefix(column, new byte[]{l});

                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, new byte[]{r});
                    byte[] endKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else { // r >= 0 && l <= -1
                    // 双端 [l, -1] & [0, r]
                    param.addOrUpdateLinePrefix(column, new byte[]{0});

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, new byte[]{r});
                    byte[] endKey1 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));

                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    param.addOrUpdateLinePrefix(column, new byte[]{l});
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(), param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, new byte[]{(byte) -1});
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }

            case 2: // short
                short ls = Bytes.toShort(lower);
                short rs = Bytes.toShort(upper);
                if (ls > Short.MAX_VALUE || rs < Short.MIN_VALUE) {
                    return null;
                }
                if (rs <= (short) -1 || ls >= (short) 0) {
                    // [ls, rs)
                    // ls++;
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(ls));

                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(rs));
                    byte[] endKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param
                                    .getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else {
                    // 双端 [ls, -1] & [0, rs]
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes((short) 0));

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(rs));
                    byte[] endKey1 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(ls));
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes((short) -1));
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }

            case 3: // int
                int li = Bytes.toInt(lower);
                int ri = Bytes.toInt(upper);
                if (li > Integer.MAX_VALUE || ri < Integer.MIN_VALUE) {
                    return null;
                }
                if (ri <= -1 || li >= 0) {
                    // [li, ri]
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(li));

                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(ri));
                    byte[] endKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param
                                    .getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else {
                    // 双端 [li, -1] & [0, ri]
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(0));

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(ri));
                    byte[] endKey1 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(li));
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(-1));
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }

            case 4: // long
                long ll = Bytes.toLong(lower);
                long rl = Bytes.toLong(upper);
                if (ll > Long.MAX_VALUE || rl < Long.MIN_VALUE) {
                    return null;
                }
                if (rl <= -1L || ll >= 0L) {
                    // [ll, rl]
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(ll));

                    byte[] startKey = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(rl));
                    byte[] endKey = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param
                                    .getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey, endKey));
                    return res;
                } else {
                    // 双端 [ll, -1] & [0, rl]
                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(0L));

                    byte[] startKey1 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(rl));
                    byte[] endKey1 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    List<KeyPair> res = new ArrayList<>();
                    res.add(new KeyPair(startKey1, endKey1));

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(ll));
                    byte[] startKey2 = ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true);

                    param.addOrUpdateLinePrefix(column, Bytes.toBytes(-1L));
                    byte[] endKey2 = ByteArrayUtils.getLargeByteArray(ByteArrayUtils.buildIndexTableScanPrefix(param.getLinePrefix(),
                            param.getHitNum(), param.getIndex(), true));
                    res.add(new KeyPair(startKey2, endKey2));
                    return res;
                }
            default:
                return null;
        }
    }

}
