package service.hbasemanager.entity.filter;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.immortalcockroach.hbaseorm.constant.CommonConstants;
import com.immortalcockroach.hbaseorm.entity.query.Expression;
import com.immortalcockroach.hbaseorm.result.ListResult;
import com.immortalcockroach.hbaseorm.util.Bytes;
import service.constants.ServiceConstants;
import service.hbasemanager.entity.index.Index;
import service.hbasemanager.entity.scanresult.IndexLine;
import service.hbasemanager.entity.scanresult.IndexScanResult;
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

    public IndexLineFilter(Map<String, Expression> expressionMap, Set<String> qualifiers) {

        this.expressionMap = expressionMap;
        this.qualifiers = qualifiers;
    }

    public void setIndex(Index index) {
        this.index = index;
    }

    public void setHitNum(int hitNum) {
        this.hitNum = hitNum;
    }

    /**
     * 将查询的结果，使用qualifiers和index对应的列进行过滤
     *
     * @param indexScanTmpRes
     * @return
     */
    public IndexScanResult filter(ListResult indexScanTmpRes) {
        JSONArray array = indexScanTmpRes.getData();
        // scanResult中的line只保留查询的列
        IndexScanResult scanResult = new IndexScanResult(index.getIndexColumnList(), qualifiers);
        int size = array.size();
        for (int i = 0; i <= size - 1; i++) {
            JSONObject line = array.getJSONObject(i);
            byte[] rowkey = line.getBytes(CommonConstants.ROW_KEY);
            // 将行键展开
            byte[][] splitArray = ByteArrayUtils.getByteListWithSeparator(rowkey, ServiceConstants.EOT, ServiceConstants.ESC);
            // 如果split之后的行符合expressions的标准，则加入ScanResult
            if (check(splitArray)) {
                // 保留查询的列
                scanResult.add(new IndexLine(splitArray, scanResult.getFilterColumn()));
            }

        }
        return scanResult;
    }

    /**
     * 根据查询条件判断是否命中该行
     *
     * @param splitArray
     * @return
     */
    public boolean check(byte[][] splitArray) {
        int size = splitArray.length;
        // 跳过头部的index序号和尾部的rowkey
        for (int i = 1; i <= size - 2; i += 2) {
            Expression expression = expressionMap.get(Bytes.toString(splitArray[i]));
            // 该列没有在查询条件中
            if (expression == null) {
                continue;
            } else {
                byte[] value = splitArray[i + 1];
                if (!ByteArrayUtils.checkValueRange(value, expression)) {
                    return false;
                }
            }
        }
        if (expressionMap.containsKey(CommonConstants.ROW_KEY)) {
            byte[] value = splitArray[size - 1];
            if (!ByteArrayUtils.checkValueRange(value, expressionMap.get(CommonConstants.ROW_KEY))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 回表查询时的行check
     *
     * @param line
     * @return
     */
    public boolean check(JSONObject line) {

        for (String key : line.keySet()) {
            Expression expression = expressionMap.get(key);
            if (expression == null) {
                continue;
            } else {
                byte[] value = line.getBytes(key);
                if (!ByteArrayUtils.checkValueRange(value, expression)) {
                    return false;
                }
            }
        }
        return true;
    }
}
