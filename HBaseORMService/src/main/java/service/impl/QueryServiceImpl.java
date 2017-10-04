package service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.immortalcockroach.hbaseorm.api.QueryService;
import com.immortalcockroach.hbaseorm.param.QueryParam;
import com.immortalcockroach.hbaseorm.result.AbstractResult;
import com.immortalcockroach.hbaseorm.result.ListResult;
import com.immortalcockroach.hbaseorm.result.PlainResult;
import com.immortalcockroach.hbaseorm.util.Bytes;
import com.immortalcockroach.hbaseorm.util.ResultUtil;
import service.hbasemanager.creation.index.GlobalIndexInfoHolder;
import service.hbasemanager.entity.filter.IndexLineFilter;
import service.hbasemanager.entity.index.Index;
import service.hbasemanager.entity.index.QueryInfoWithIndexes;
import service.hbasemanager.entity.indexresult.IndexLine;
import service.hbasemanager.entity.indexresult.MergedResult;
import service.hbasemanager.entity.indexresult.TableScanParam;
import service.hbasemanager.read.TableGetService;
import service.hbasemanager.read.TableScanService;
import service.hbasemanager.utils.HBaseTableUtils;
import service.utils.ByteArrayUtils;
import service.utils.IndexUtils;

import javax.annotation.Resource;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 查询API的实现，目前只有范围查询，后面会加上各种过滤的条件
 * Created by immortalCockroach on 8/29/17.
 */
public class QueryServiceImpl implements QueryService {

    @Resource
    private TableScanService scanner;

    @Resource
    private TableGetService getter;

    @Resource
    private GlobalIndexInfoHolder indexInfoHolder;

    @Override
    public AbstractResult query(QueryParam queryParam) {
        byte[] tableName = queryParam.getTableName();
        // 检测表是否存在
        if (!HBaseTableUtils.tableExists(tableName)) {
            return ResultUtil.getFailedPlainResult("表" + Bytes.toString(tableName) + "不存在");
        }

        // 存在的索引信息
        List<Index> existedIndex = indexInfoHolder.getTableIndexes(tableName);

        Set<String> qualifiers = new HashSet<>(Arrays.asList(queryParam.getQualifiers()));

        // 获得每个索引的命中信息
        int[] hitIndexNums = IndexUtils.getHitIndexWhenQuery(existedIndex, queryParam.getQueryColumns());
        // 直接全表扫描
        if (!IndexUtils.hitAny(hitIndexNums)) {
            return null;
        } else {
            QueryInfoWithIndexes queryInfoWithIndexes = new QueryInfoWithIndexes(existedIndex, queryParam.getCondition().getExpressions(), hitIndexNums);
            // size代表该表的索引数量
            int size = hitIndexNums.length;
            MergedResult mergedResult = new MergedResult(qualifiers);
            IndexLineFilter filter = new IndexLineFilter(queryInfoWithIndexes.getExpressionMap(), qualifiers);
            for (int i = 0; i <= size - 1; i++) {
                // 代表该索引没有被命中，跳过
                if (hitIndexNums[i] == 0) {
                    continue;
                }
                // TODO: 2017-09-21 根据索引命中信息构建index表的扫描
                TableScanParam param = queryInfoWithIndexes.buildIndexTableQueryPrefix(i, hitIndexNums[i]);
                ListResult result = scanner.scan(ByteArrayUtils.getIndexTableName(tableName), param);
                if (!result.getSuccess() || result.getSize() == 0) {
                    return ResultUtil.getEmptyListResult();
                }
                filter.setIndex(existedIndex.get(i));
                filter.setHitNum(hitIndexNums[i]);
                mergedResult.merge(filter.filter(result));
                // 合并后结果为0，则直接返回list
                if (mergedResult.getResultSize() == 0) {
                    return ResultUtil.getEmptyListResult();
                }

            }
            Set<String> unhitColumns = queryInfoWithIndexes.getUnhitColumns();
            Set<String> leftColumns = mergedResult.getLeftColumns();
            // 说明不需要回表查询
            if (unhitColumns.size() == 0 && leftColumns.size() == 0) {
                return buildResult(mergedResult.getMergedLineMap());
            } else {
                LinkedHashMap<ByteBuffer, IndexLine> mergedMap = mergedResult.getMergedLineMap();
                // 构造回表查询的列，然后回表查询
                String[] backTableQualifiers = buildQualifiersForBackTable(unhitColumns, leftColumns);

                Iterator<Map.Entry<ByteBuffer, IndexLine>> iterator = mergedMap.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<ByteBuffer, IndexLine> entry = iterator.next();
                    // 如果不在合并的结果里，则remove
                    ByteBuffer rowkey = entry.getKey();
                    PlainResult backTableLine = getter.read(tableName, rowkey.array(), backTableQualifiers);
                    if (!backTableLine.getSuccess() || backTableLine.getSize() == 0) {
                        iterator.remove();
                        continue;
                    }
                    // 验证回表查询的结果，然后和当前的line合并
                    JSONObject line = backTableLine.getData();
                    if (!filter.check(line)) {
                        iterator.remove();
                        continue;
                    } else {
                        // 将2个结果merge
                        IndexLine oldLine = entry.getValue();
                        oldLine.mergeLine(line);
                    }
                }
                return buildResult(mergedMap);
            }
        }
    }

    /**
     * 将结果转换为相应的形式
     *
     * @param mergedResult
     * @return
     */
    private AbstractResult buildResult(LinkedHashMap<ByteBuffer, IndexLine> mergedResult) {
        JSONArray array = new JSONArray();
        for (IndexLine indexLine : mergedResult.values()) {
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
    private String[] buildQualifiersForBackTable(Set<String> unhitColumns, Set<String> leftColumns) {
        Set<String> res = new HashSet<>();
        res.addAll(unhitColumns);
        res.addAll(leftColumns);
        return res.toArray(new String[]{});
    }
}
