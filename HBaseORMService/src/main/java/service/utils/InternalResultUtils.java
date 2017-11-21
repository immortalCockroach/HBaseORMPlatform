package service.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.immortalcockroach.hbaseorm.constant.CommonConstants;
import com.immortalcockroach.hbaseorm.result.ListResult;
import com.immortalcockroach.hbaseorm.util.ResultUtil;
import service.hbasemanager.entity.filter.IndexLineFilter;
import service.hbasemanager.entity.scanresult.IndexLine;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;

public class InternalResultUtils {
    /**
     * 将结果转换为相应的形式
     *
     * @param mergedResult
     * @return
     */
    public static ListResult buildResult(LinkedHashMap<ByteBuffer, IndexLine> mergedResult, boolean containsRowkey,
                                         ListResult backTableRes, IndexLineFilter filter, boolean removed) {
        JSONArray array = new JSONArray();
        JSONArray scanRes = backTableRes.getData();
        int size = scanRes.size();
        for (int i = 0; i <= size - 1; i++) {
            JSONObject readLine = scanRes.getJSONObject(i);
            if (!filter.check(readLine, removed)) {
                continue;
            }
            byte[] rowkey = readLine.getBytes(CommonConstants.ROW_KEY);
            IndexLine originLine = mergedResult.get(ByteBuffer.wrap(rowkey));
            if (originLine == null) {
                continue;
            }

            readLine.putAll(originLine.toJSONObject());
            array.add(readLine);
        }

        return ResultUtil.getSuccessListResult(array);
    }

    public static ListResult buildResult(LinkedHashMap<ByteBuffer, IndexLine> mergedResult, boolean containsRowkey) {
        JSONArray array = new JSONArray();
        for (IndexLine indexLine : mergedResult.values()) {
            JSONObject tmp = indexLine.toJSONObject();
            // 由于IndexLine的map不包含rowkey，如果查询结果中要求rowkey的话，单独加上
            if (containsRowkey) {
                tmp.put(CommonConstants.ROW_KEY, indexLine.getRowkey());
            }
            array.add(indexLine.toJSONObject());
        }
        return ResultUtil.getSuccessListResult(array);
    }


    /**
     * 根据需要filter的列和待查询的列构造回表查询的qualifiers
     *
     * @param unhitColumns
     * @param leftColumns
     * @return
     */
    public static String[] buildQualifiersForBackTable(Set<String> unhitColumns, Set<String> leftColumns) {
        Set<String> res = new HashSet<>();
        res.addAll(unhitColumns);
        res.addAll(leftColumns);
        return res.toArray(new String[]{});
    }
}
