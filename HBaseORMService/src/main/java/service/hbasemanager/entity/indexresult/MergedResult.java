package service.hbasemanager.entity.indexresult;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * 多条件查询的merge结果
 */
public class MergedResult {
    private Set<ByteBuffer> rowkeys;
    // 代表还需要合并的列
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
            // 移除掉已经查询到的列
            leftColumns.removeAll(indexScanResult.getFilterColumn());
            this.rowkeys = indexScanResult.getRowkeys();
            Map<ByteBuffer, IndexLine> map = indexScanResult.getLineMap();
            for (Map.Entry<ByteBuffer, IndexLine> entry : map.entrySet()) {
                mergedLineMap.put(entry.getKey(), entry.getValue());
            }
        } else {
            // 将新查询到的列从回表列中移除
            leftColumns.removeAll(indexScanResult.getFilterColumn());

            // 求已有id和新id的交集
            rowkeys.retainAll(indexScanResult.getRowkeys());
            // 合并后无结果
            if (rowkeys.size() == 0) {
                mergedLineMap.clear();
                return;
            }
            Iterator<Map.Entry<ByteBuffer, IndexLine>> iterator = mergedLineMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<ByteBuffer, IndexLine> entry = iterator.next();
                // 如果不在合并的结果里，则remove
                if (!rowkeys.contains(entry.getKey())) {
                    iterator.remove();
                }
                // 如果所有的待查询的列都查询到了，后面不需要根据查询扩展行，只需要根据rowkey求交集即可
                if (leftColumns.size() > 0) {
                    IndexLine line = entry.getValue();
                    // 将2个共同的rowkey的行进行拼接
                    line.mergeLine(indexScanResult.getLineMap().get(entry.getKey()));
                }
            }

        }
    }

    public int getResultSize() {
        return mergedLineMap.size();
    }
}
