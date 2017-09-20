package service.hbasemanager.entity.indexresult;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 单个索引命中后的结果
 */
public class IndexScanResult {
    // 行键集合
    private Set<ByteBuffer> rowkeys;
    // 行键与对应的行
    private Map<ByteBuffer, IndexLine> lineMap;
    // 查询结果中根据qualifiers筛出需要保留的列
    private Set<String> filterColumn;

    public IndexScanResult(List<String> indexColumns, Set<String> qualifiers) {
        rowkeys = new HashSet<>();
        lineMap = new HashMap<>();
        filterColumn = new HashSet<>();
        // 根据查询的列，和表中已有的列得到需要保留的列
        for (String column : indexColumns) {
            if (qualifiers.contains(column)) {
                filterColumn.add(column);
            }
        }
    }

    public Set<ByteBuffer> getRowkeys() {
        return rowkeys;
    }

    public Map<ByteBuffer, IndexLine> getLineMap() {
        return lineMap;
    }

    public Set<String> getFilterColumn() {
        return filterColumn;
    }

    public void add(IndexLine line) {
        ByteBuffer rowkey = ByteBuffer.wrap(line.getRowkey());
        rowkeys.add(rowkey);
        lineMap.put(rowkey, line);
    }
}
