package service.hbasemanager.creation.table.splitalgorithms;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;

import java.math.BigInteger;

/**
 * UUID字节分割的类
 * Created by immortalCockroach on 8/29/17.
 */
public class UUIDStringSplitor implements RegionSplitter.SplitAlgorithm {
    // UUID的最大和最小，此处去掉了UUID中的4个'-'，替换为0，为了下面的分割的方便
    final static String DEFAULT_MIN_HEX = "000000000000000000000000000000000000";
    final static String DEFAULT_MAX_HEX = "FFFFFFFF0FFFF0FFFF0FFFF0FFFFFFFFFFFF";

    String firstRow = DEFAULT_MIN_HEX;
    BigInteger firstRowInt = BigInteger.ZERO;
    String lastRow = DEFAULT_MAX_HEX;
    BigInteger lastRowInt = new BigInteger(lastRow, 16);
    int rowComparisonLength = lastRow.length();

    public static byte[] convertToByte(BigInteger bigInteger, int pad) {
        String bigIntegerString = bigInteger.toString(16);
        // 左填充0补齐
        bigIntegerString = StringUtils.leftPad(bigIntegerString, pad, '0');
        // 将UUID中的8 12 16 20这4个下标的字符替换为'-' 转换为标准的UUID形式
        return Bytes.toBytes(UUIDFormat(bigIntegerString));
    }

    /**
     * 将字节分割时的splitString(长度为36)替换为标准的UUID形式，即将
     * @param splitString
     * @return
     */
    private static String UUIDFormat(String splitString) {
        char[] uuid = splitString.toCharArray();
        uuid[8] = '-';
        uuid[12] = '-';
        uuid[16] = '-';
        uuid[20] = '-';
        return new String(uuid);
    }

    /**
     * 目前没有针对字节空间上下界划分的方法，故该方法不实现
     * @param start
     * @param end
     * @return
     */
    @Override
    public byte[] split(byte[] start, byte[] end) {
        throw new UnsupportedOperationException("暂不支持");
    }


    /**
     * 将UUID的字节空间进行切分
     *
     * @param n
     * @return
     */
    @Override
    public byte[][] split(int n) {
        // 加1是为了计算整个字节空间的字节长度
        BigInteger range = lastRowInt.subtract(firstRowInt).add(BigInteger.ONE);
        // n - 1个值隔断成为n个空间
        BigInteger[] splits = new BigInteger[n - 1];
        // 每个空间的大小
        BigInteger sizeOfEachSplit = range.divide(BigInteger.valueOf(n));

        // 填充n - 1个隔断值
        for (int i = 1; i < n; i++) {
            splits[i - 1] = firstRowInt.add(sizeOfEachSplit.multiply(BigInteger
                    .valueOf(i)));
        }
        return convertToBytes(splits);
    }

    @Override
    public byte[] firstRow() {
        return convertToByte(firstRowInt);
    }

    @Override
    public byte[] lastRow() {
        return convertToByte(lastRowInt);
    }

    @Override
    public void setFirstRow(String userInput) {
        firstRow = userInput;
        firstRowInt = new BigInteger(firstRow, 16);
    }

    @Override
    public void setLastRow(String userInput) {
        lastRow = userInput;
        lastRowInt = new BigInteger(lastRow, 16);
        rowComparisonLength = lastRow.length();
    }

    @Override
    public byte[] strToRow(String in) {
        return convertToByte(new BigInteger(in, 16));
    }

    @Override
    public String rowToStr(byte[] row) {
        return Bytes.toStringBinary(row);
    }

    @Override
    public String separator() {
        return " ";
    }

    @Override
    public void setFirstRow(byte[] userInput) {
        firstRow = Bytes.toString(userInput);
    }

    @Override
    public void setLastRow(byte[] userInput) {
        lastRow = Bytes.toString(userInput);
    }

    public byte[][] convertToBytes(BigInteger[] bigIntegers) {
        byte[][] returnBytes = new byte[bigIntegers.length][];
        for (int i = 0; i < bigIntegers.length; i++) {
            returnBytes[i] = convertToByte(bigIntegers[i]);
        }
        return returnBytes;
    }

    public byte[] convertToByte(BigInteger bigInteger) {
        return convertToByte(bigInteger, rowComparisonLength);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " [" + rowToStr(firstRow())
                + "," + rowToStr(lastRow()) + "]";
    }
}
