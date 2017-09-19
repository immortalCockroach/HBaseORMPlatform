package service.hbasemanager.entity.indexresult;

import com.immortalcockroach.hbaseorm.util.Bytes;

import java.util.Map;
import java.util.Set;

/**
 * 索引表行键
 */
public class IndexLine {
    private byte[] rowkey;
    private Map<String, byte[]> columnMap;

    public IndexLine(byte[][] splitArray, Set<String> filterColumns) {
        int size = splitArray.length;
        for (int i = 1; i <= size - 2; i += 2) {
            String column = Bytes.toString(splitArray[i]);
            if (filterColumns.contains(column)) {
                columnMap.put(column, splitArray[i + 1]);
            }
        }
        rowkey = splitArray[size - 1];
    }

    public byte[] getRowkey() {
        return rowkey;
    }

    public Map<String, byte[]> getColumnMap() {
        return columnMap;
    }
}
