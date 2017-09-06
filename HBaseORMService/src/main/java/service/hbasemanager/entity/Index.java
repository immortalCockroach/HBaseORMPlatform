package service.hbasemanager.entity;

import com.immortalcockroach.hbaseorm.util.Bytes;

import java.util.Map;

/**
 * 代表一个索引表的rowkey
 * Created by immortalCockroach on 9/1/17.
 */
public class Index {
    private Map<String, byte[]> columnsMap;
    private byte[] rowkey;

    /**
     * 保证是奇数长度
     * 格式为col1 col1v... col2 col2v... rowkey
     *
     * @param array
     */
    public Index(byte[][] array) {
        int size = array.length;
        for (int i = 0; i <= size - 2; i += 2) {
            columnsMap.put(Bytes.toString(array[i]), array[i + 1]);
        }
        // 最后一个为rowkey
        rowkey = array[size - 1];
    }

    public byte[] getRowkey() {
        return rowkey;
    }

    public byte[] getColValueByQualifier(String qualifier) {
        return columnsMap.get(qualifier);
    }
}
