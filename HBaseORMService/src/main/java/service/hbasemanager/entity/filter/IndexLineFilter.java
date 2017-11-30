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
import service.hbasemanager.entity.tabldesc.TableDescriptor;
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
    // 查询的列
    private Set<String> qualifiers;
    private TableDescriptor descriptor;

    public IndexLineFilter(Map<String, Expression> expressionMap, Set<String> qualifiers, TableDescriptor descriptor) {

        this.expressionMap = expressionMap;
        this.qualifiers = qualifiers;
        this.descriptor = descriptor;
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
            byte[][] splitArray = ByteArrayUtils.getByteListWithSeparator(rowkey, index);
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
            String column = Bytes.toString(splitArray[i]);
            Expression expression = expressionMap.get(column);
            // 该列没有在查询条件中
            if (expression == null) {
                continue;
            } else {
                byte[] value = splitArray[i + 1];
                if (!ByteArrayUtils.checkValueRange(value, expression, descriptor.getTypeOfColumn(column))) {
                    return false;
                }
            }
        }
        // 如果有rowkey的过滤，则在此处过滤
        if (expressionMap.containsKey(CommonConstants.ROW_KEY)) {
            byte[] value = splitArray[size - 1];
            if (!ByteArrayUtils.checkValueRange(value, expressionMap.get(CommonConstants.ROW_KEY),
                    descriptor.getTypeOfColumn(CommonConstants.ROW_KEY))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 回表查询时的行check
     * 验证过后的line为去除非查询的列的JSONObject
     *
     * @param line
     * @param remove 代表是否要移除掉不在qualifier中的列，如果是查询，则移除；如果是删除或者更新，则不删除
     * @return
     */
    public boolean check(JSONObject line, boolean remove) {

        for (String column : line.keySet()) {
            Expression expression = expressionMap.get(column);
            byte[] value = line.getBytes(column);
            // 查询到的列不是过滤条件中的列
            if (expression == null) {
                continue;
            } else {
                if (!ByteArrayUtils.checkValueRange(value, expression, descriptor.getTypeOfColumn(column))) {
                    return false;
                } else {
                    // 如果不在qualiiers中，则剔除该列
                    if (remove && !qualifiers.contains(column)) {
                        line.remove(column);
                    }
                }
            }
        }
        return true;
    }
}
