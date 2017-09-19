package service.hbasemanager.entity.indexresult;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * 多条件查询的merge结果
 */
public class MergedResult {
    private Set<ByteBuffer> rowkeys;
    private Set<String> leftColumns;

    private LinkedHashMap<ByteBuffer, IndexLine> mergedLineMap;

    public MergedResult(Set<String> qualifiers) {
        rowkeys = new HashSet<>();
        // 因为leftColumn需要修改，因此此处的hashSet和公用的隔离
        leftColumns = new HashSet<>(qualifiers);
        mergedLineMap = new LinkedHashMap<>();
    }

    public Set<ByteBuffer> getRowkeys() {
        return rowkeys;
    }

    public Set<String> getLeftColumns() {
        return leftColumns;
    }

    public LinkedHashMap<ByteBuffer, IndexLine> getMergedLineMap() {
        return mergedLineMap;
    }

    public void merge(IndexScanResult indexScanResult) {
        // 初始化
        if (this.getResultSize() == 0) {
            this.rowkeys = indexScanResult.getRowkeys();
            Map<ByteBuffer, IndexLine> map = indexScanResult.getLineMap();
            for (Map.Entry<ByteBuffer, IndexLine> entry : map.entrySet()) {
                mergedLineMap.put(entry.getKey(), entry.getValue());
            }
            // 移除掉已经查询到的列
            leftColumns.removeAll(indexScanResult.getFilterColumn());
        } else {
            
        }
    }

    public int getResultSize() {
        return mergedLineMap.size();
    }
}
