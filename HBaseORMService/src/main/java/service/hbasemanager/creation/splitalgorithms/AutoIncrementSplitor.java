package service.hbasemanager.creation.splitalgorithms;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;

/**
 * 自增ID主键的split，根据Upbound的大小选择short空间和int空间
 * long空间暂时不考虑
 * Created by immortalCockroach on 8/29/17.
 */
public class AutoIncrementSplitor implements RegionSplitter.SplitAlgorithm {

    /**
     * 采用ThreadLocal来区分每个线程创建表时lowerBound和upperBound的value
     */
    private ThreadLocal<Integer> upperBoundLocal = new ThreadLocal<>();
    private ThreadLocal<Integer> lowerBoundLocal = new ThreadLocal<>();

    public byte[] convertToByte(Integer i) {
        // 此处根据upperBound的值直接分割整形的字节空间即可。
        if (upperBoundLocal.get() > Short.MAX_VALUE) {
            return Bytes.toBytes(i.intValue());
        } else {
            return Bytes.toBytes(i.shortValue());
        }

    }

    /**
     * 调用split的时候默认设置的upperBound值
     *
     * @param upperBound
     */
    public void setUpperBound(Integer upperBound) {
        upperBoundLocal.set(upperBound);
    }


    /**
     * 调用split的时候默认设置的lowerBound值
     *
     * @param lowerBound
     */
    public void setLowerBound(Integer lowerBound) {
        lowerBoundLocal.set(lowerBound);
    }


    /**
     * 目前没有针对字节空间上下界划分的方法，故该方法不实现
     *
     * @param start
     * @param end
     * @return
     */
    @Override
    public byte[] split(byte[] start, byte[] end) {
        throw new UnsupportedOperationException("暂不支持");
    }


    /**
     * 将自增主键的字节空间进行切分，和UUID的隔断值生成方式完全一样
     *
     * @param n
     * @return
     */
    @Override
    public byte[][] split(int n) {
        // 加1是为了计算整个字节空间的字节长度
        int firstRowInt = lowerBoundLocal.get();
        int lastRowInt = upperBoundLocal.get();

        int range = lastRowInt - firstRowInt + 1;
        // n - 1个值隔断成为n个空间
        int[] splits = new int[n - 1];
        // 每个空间的大小
        int sizeOfEachSplit = range / n;

        // 填充n - 1个隔断值
        for (int i = 1; i < n; i++) {
            splits[i - 1] = firstRowInt + sizeOfEachSplit * i;
        }
        return convertToBytes(splits);
    }

    @Override
    public byte[] firstRow() {
        return convertToByte(lowerBoundLocal.get());
    }

    @Override
    public byte[] lastRow() {
        return convertToByte(upperBoundLocal.get());
    }

    @Override
    public void setFirstRow(String userInput) {
        int firstRowInt = Integer.valueOf(userInput);
        lowerBoundLocal.set(firstRowInt);
    }

    @Override
    public void setLastRow(String userInput) {
        int lastRowInt = Integer.valueOf(userInput);
        upperBoundLocal.set(lastRowInt);
    }

    @Override
    public byte[] strToRow(String in) {
        return convertToByte(Integer.valueOf(in, 16));
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
        int firstRowInt = Integer.valueOf(Bytes.toString(userInput));
        lowerBoundLocal.set(firstRowInt);
    }

    @Override
    public void setLastRow(byte[] userInput) {
        int lastRowInt = Integer.valueOf(Bytes.toString(userInput));
        upperBoundLocal.set(lastRowInt);
    }

    public byte[][] convertToBytes(int[] integers) {
        byte[][] returnBytes = new byte[integers.length][];
        for (int i = 0; i < integers.length; i++) {
            returnBytes[i] = convertToByte(integers[i]);
        }
        return returnBytes;
    }


    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " [" + rowToStr(firstRow())
                + "," + rowToStr(lastRow()) + "]";
    }
}
