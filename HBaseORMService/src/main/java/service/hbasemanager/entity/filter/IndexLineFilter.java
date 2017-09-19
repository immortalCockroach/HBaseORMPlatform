package service.hbasemanager.entity.filter;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.immortalcockroach.hbaseorm.constant.CommonConstants;
import com.immortalcockroach.hbaseorm.param.condition.Expression;
import com.immortalcockroach.hbaseorm.result.ListResult;
import com.immortalcockroach.hbaseorm.util.Bytes;
import service.constants.ServiceConstants;
import service.hbasemanager.entity.index.Index;
import service.hbasemanager.entity.indexresult.IndexLine;
import service.hbasemanager.entity.indexresult.IndexScanResult;
import service.utils.ByteArrayUtils;

import java.util.Map;
import java.util.Set;

/**
 * 根据索引的命中结果，对命中的行的信息做过滤
 */
public class IndexLineFilter {
    private Index index;
    private int hitNum;
    private Map<String, Expression> expressionMap;
    private Set<String> qualifiers;

    public IndexLineFilter(Index index, int hitNum, Map<String, Expression> expressionMap, Set<String> qualifiers) {
        this.index = index;
        this.hitNum = hitNum;
        this.expressionMap = expressionMap;
        this.qualifiers = qualifiers;
    }

    public IndexScanResult filter(ListResult indexScanTmpRes) {
        JSONArray array = indexScanTmpRes.getData();
        IndexScanResult scanResult = new IndexScanResult(index.getIndexColumnList(), qualifiers);
        int size = array.size();
        for (int i = 0; i <= size - 1; i++) {
            JSONObject line = array.getJSONObject(i);
            byte[] rowkey = line.getBytes(CommonConstants.ROW_KEY);
            byte[][] splitArray = ByteArrayUtils.getByteListWithSeparator(rowkey, ServiceConstants.EOT, ServiceConstants.ESC, ServiceConstants.NUL);
            if (check(splitArray)) {
                scanResult.add(new IndexLine(splitArray, scanResult.getFilterColumn()));
            }

        }
        return scanResult;
    }

    /**
     * 根据查询条件判断是否命中该行
     * @param splitArray
     * @return
     */
    public boolean check(byte[][] splitArray) {
        int size = splitArray.length;
        // 跳过头部的index序号和尾部的rowkey
        for (int i = 1; i <= size - 2; i += 2) {
            String column = Bytes.toString(splitArray[i]);
            byte[] value = splitArray[i + 1];
            Expression expression = expressionMap.get(column);
            // 该列没有在查询条件中
            if (expression == null) {
                continue;
            } else {
                if (!ByteArrayUtils.checkValueRange(value, expression)) {
                    return false;
                }
            }
        }
        return true;
    }
}
