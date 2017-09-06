package service.utils;

import com.alibaba.fastjson.JSONObject;
import com.immortalcockroach.hbaseorm.constant.CommonConstants;
import com.immortalcockroach.hbaseorm.util.Bytes;
import org.apache.commons.lang.ArrayUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
    public static int preProcessEscapeCharacterOfBytes(byte[][] list, byte separator, byte escape, byte nul) {
        int size = list.length;
        int length = 0;
        for (int i = 0; i <= size - 1; i++) {
            List<Byte> tmp = new ArrayList<>();
            byte[] array = list[i];
            // 有可能某个列是null，此时填充ESC NULL
            if (array == null) {
                tmp.add(escape);
                tmp.add(nul);
            } else {
                for (byte b : array) {
                    // 转义符或者分隔符前面加上转义，即ESC->ESC ESC, EOT->ESC EOT
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
    public static byte[] concat(byte[][] list, byte separator, int length) {
        int size = list.length;
        // length个字节的长度 加上size - 1个分隔符的长度
        byte[] res = new byte[length + size - 1];
        int index = 0;
        for (int i = 0; i <= size - 2; i++) {
            byte[] col = list[i];

            System.arraycopy(col, 0, res, index, col.length);
            index += col.length;

            res[index] = separator;
            index++;
        }
        // 拷贝最后一个 末尾不带分隔符
        System.arraycopy(list[size - 1], 0, res, index, list[size - 1].length);

        return res;

    }

    /**
     * 无EOT的拼接
     *
     * @param list
     * @param length
     * @return
     */
    public static byte[] concat(byte[][] list, int length) {
        int size = list.length;
        // length个字节的长度 加上size - 1个分隔符的长度
        byte[] res = new byte[length];
        int index = 0;
        for (int i = 0; i <= size - 1; i++) {
            byte[] col = list[i];

            System.arraycopy(col, 0, res, index, col.length);
            index += col.length;
        }

        return res;
    }


    /**
     * 将jsonObject的按照qualifiers的顺序组成一个byte[][]
     * 格式为col1 col1v... col2 col2v... rowkey
     *
     * @param line
     * @param qualifiers 列修饰符 不包含rowkey
     * @return
     */
    public static byte[][] jsonObjectToByteArrayList(JSONObject line, String[] qualifiers) {
        int size = qualifiers.length;
        // 总长度为qualifies.length * 2  + 1对应 length * 2个col + colv 以及1个rowkey的值
        byte[][] res = new byte[size * 2 + 1][];
        for (int i = 0; i <= size - 1; i++) {
            res[2 * i] = Bytes.toBytes(qualifiers[i]);
            res[2 * i + 1] = line.getBytes(qualifiers[i]);
        }
        // 最后一个为rowkey的值
        res[2 * size] = line.getBytes(CommonConstants.ROW_KEY);

        return res;
    }

    public static byte[][] jsonObjectToByteArrayList(Map<String, byte[]> line, String[] qualifiers) {
        int size = qualifiers.length;
        // 总长度为qualifies.length * 2  + 1对应 length * 2个col + colv 以及1个rowkey的值
        byte[][] res = new byte[size * 2 + 1][];
        for (int i = 0; i <= size - 1; i++) {
            res[2 * i] = Bytes.toBytes(qualifiers[i]);
            res[2 * i + 1] = line.get(qualifiers[i]);
        }
        // 最后一个为rowkey的值
        res[2 * size] = line.get(CommonConstants.ROW_KEY);

        return res;
    }

    /**
     * 根据行的JSONObject和qualifiers的顺序，将一行使用separator进行拼接
     * 需要对内容中的separator和escape字节数据进行转义
     *
     * @param line
     * @param separator  分隔符EOT
     * @param escape     转义符ESC
     * @param qualifiers
     * @return
     */
    public static byte[] generateIndexRowKey(JSONObject line, String[] qualifiers, byte separator, byte escape, byte nul) {
        // 将一个数据变为col1 col2 ... col1v col2v ... rowkey的形式
        byte[][] list = jsonObjectToByteArrayList(line, qualifiers);
        // 将每个byte[]做转义字符处理
        int length = preProcessEscapeCharacterOfBytes(list, separator, escape, nul);
        // 将list使用separator进行拼接
        byte[] indexRowkey = concat(list, separator, length);
        return indexRowkey;
    }

    public static byte[] generateIndexRowKey(Map<String, byte[]> line, String[] qualifiers, byte separator, byte
            escape, byte nul) {
        // 将一个数据变为col1 col2 ... col1v col2v ... rowkey的形式
        byte[][] list = jsonObjectToByteArrayList(line, qualifiers);
        // 将每个byte[]做转义字符处理
        int length = preProcessEscapeCharacterOfBytes(list, separator, escape, nul);
        // 将list使用separator进行拼接
        byte[] indexRowkey = concat(list, separator, length);
        return indexRowkey;
    }

    /**
     * 根据Separator将字节数组进行分割
     * 1、将字节数组按照单独的EOT进行分割
     * 2、将分割后的数组进行转义的逆操作
     *
     * @param array
     * @param separator
     * @param escape
     * @return
     */
    public static byte[][] getByteListWithSeparator(byte[] array, byte separator, byte escape, byte nul) {
        byte[][] tmp = splitArrayWithStandAloneSeprator(array, separator, escape);
        removeEscapeCharacter(tmp, escape, nul);
        return tmp;
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
    public static byte[][] splitArrayWithStandAloneSeprator(byte[] array, byte separator, byte escape) {
        List<byte[]> list = new ArrayList<>();
        int size = array.length;
        int start = 0, index = 0;
        while (index <= size - 1) {
            byte content = array[index];
            if (content == separator) {
                // 说明该分隔符被转义，需要跳过(EOT不可能出现在起始位置)
                if (array[index - 1] == escape) {
                    index++;
                } else {
                    // 说明是真正的分隔符，此时将start, index - 1之间一共index - start的字节拷贝
                    byte[] tmp = new byte[index - start];
                    System.arraycopy(array, start, tmp, 0, index - start);
                    list.add(tmp);
                    // 跳过分隔符，继续到下一个字节
                    index++;
                    start = index;
                }
            } else {
                index++;
            }
        }
        // 拷贝最后一段
        byte[] tmp = new byte[index - start];
        System.arraycopy(array, start, tmp, 0, index - start);
        list.add(tmp);

        byte[][] res = new byte[list.size()][];
        return list.toArray(res);
    }

    /**
     * 去掉转义字符
     * 1、去掉ESC或者EOT之前的ESC
     * 2、如果一个byte[]中只有ESC nul，则说明之前是nul，此时直接将对应位置的置位null
     *
     * @param array
     * @return
     */
    public static void removeEscapeCharacter(byte[][] array, byte escape, byte nul) {
        int size = array.length;
        for (int i = 0; i <= size - 1; i++) {
            byte[] tmp = array[i];
            // 原先为null的列单独处理
            if (tmp.length == 2 && tmp[0] == escape && tmp[1] == nul) {
                array[i] = null;
            } else {
                List<Byte> list = new ArrayList<>();
                int length = tmp.length;
                for (int j = 0; j <= length - 1; j++) {
                    byte b = tmp[j];
                    // 遇到了转义字符，则将其后面一个加入
                    if (b == escape) {
                        j++;
                        list.add(tmp[j]);
                    } else {
                        list.add(b);
                    }
                }
                Byte[] newArray = new Byte[list.size()];
                list.toArray(newArray);
                array[i] = ArrayUtils.toPrimitive(newArray);
            }
        }

    }
}
