package service.hbasemanager.entity.scanresult;

import com.alibaba.fastjson.JSONObject;
import com.immortalcockroach.hbaseorm.util.Bytes;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 索引表行键展开后的类
 */
public class IndexLine {
    private byte[] rowkey;
    private Map<String, byte[]> columnMap;

    public IndexLine(byte[][] splitArray, Set<String> filterColumns) {
        columnMap = new HashMap<>();
        int size = splitArray.length;
        // 第一个位置为IndexNum，故跳过
        for (int i = 1; i <= size - 2; i += 2) {
            String column = Bytes.toString(splitArray[i]);
            // 保留在filterColumns中的列
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

    /**
     * 将newLine合并到当前的line中
     *
     * @param newLine
     */
    public void mergeLine(IndexLine newLine) {
        Map<String, byte[]> newData = newLine.getColumnMap();
        for (Map.Entry<String, byte[]> entry : newData.entrySet()) {
            String column = entry.getKey();
            // 如果列没有在原先的行中则加入
            if (columnMap.containsKey(column)) {
                continue;
            } else {
                columnMap.put(column, entry.getValue());
            }
        }
    }


    public void mergeLine(JSONObject newLine) {
        for (String key : newLine.keySet()) {
            this.columnMap.put(key, newLine.getBytes(key));
        }
    }

    public JSONObject toJSONObject() {
        JSONObject res = new JSONObject();
        for (Map.Entry<String, byte[]> entry : columnMap.entrySet()) {
            res.put(entry.getKey(), entry.getValue());
        }
        return res;
    }
}
