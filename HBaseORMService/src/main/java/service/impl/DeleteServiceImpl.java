package service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.immortalcockroach.hbaseorm.api.DeleteService;
import com.immortalcockroach.hbaseorm.constant.CommonConstants;
import com.immortalcockroach.hbaseorm.param.DeleteParam;
import com.immortalcockroach.hbaseorm.result.AbstractResult;
import com.immortalcockroach.hbaseorm.result.BaseResult;
import com.immortalcockroach.hbaseorm.result.ListResult;
import com.immortalcockroach.hbaseorm.result.PlainResult;
import com.immortalcockroach.hbaseorm.util.Bytes;
import com.immortalcockroach.hbaseorm.util.ResultUtil;
import service.hbasemanager.creation.index.GlobalIndexInfoHolder;
import service.hbasemanager.creation.tabledesc.GlobalTableDescInfoHolder;
import service.hbasemanager.deletion.TableDeleteService;
import service.hbasemanager.entity.filter.IndexLineFilter;
import service.hbasemanager.entity.index.Index;
import service.hbasemanager.entity.index.QueryInfoWithIndexes;
import service.hbasemanager.entity.scanparam.KeyPair;
import service.hbasemanager.entity.scanparam.TableScanParam;
import service.hbasemanager.entity.scanresult.IndexLine;
import service.hbasemanager.entity.scanresult.MergedResult;
import service.hbasemanager.entity.tabldesc.TableDescriptor;
import service.hbasemanager.read.TableGetService;
import service.hbasemanager.read.TableScanService;
import service.hbasemanager.utils.HBaseTableUtils;
import service.utils.ByteArrayUtils;
import service.utils.IndexUtils;
import service.utils.InternalResultUtils;

import javax.annotation.Resource;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DeleteServiceImpl implements DeleteService {

    @Resource
    private TableDeleteService tableDeleteService;

    @Resource
    private TableScanService scanner;

    @Resource
    private TableGetService getter;

    @Resource
    private GlobalIndexInfoHolder indexInfoHolder;

    @Resource
    private GlobalTableDescInfoHolder descInfoHolder;


    @Override
    public BaseResult delete(DeleteParam deleteParam) {
        byte[] tableName = deleteParam.getTableName();
        // 检测表是否存在
        if (!HBaseTableUtils.tableExists(tableName)) {
            return ResultUtil.getFailedBaseResult("表" + Bytes.toString(tableName) + "不存在");
        }

        // 存在的索引信息
        List<Index> existedIndex = indexInfoHolder.getTableIndexes(tableName);

        TableDescriptor descriptor = descInfoHolder.getDescriptor(tableName);


        // 获得每个索引的命中信息
        int[] hitIndexNums = IndexUtils.getHitIndexWhenQuery(existedIndex, deleteParam.getConditionColumnsType());
        // 直接全表扫描
        if (!IndexUtils.hitAnyIdex(hitIndexNums)) {
            return null;
        } else {
            QueryInfoWithIndexes queryInfoWithIndexes = new QueryInfoWithIndexes(existedIndex, deleteParam.getCondition()
                    .getExpressions(), hitIndexNums);
            // size代表该表的索引数量
            int size = hitIndexNums.length;
            // 传入的参数不需要筛选列，只需要保留rowkey即可
            MergedResult mergedResult = new MergedResult(new HashSet<String>());
            // 传入的参数不需要筛选列，只需要保留rowkey即可
            IndexLineFilter filter = new IndexLineFilter(queryInfoWithIndexes.getExpressionMap(), new HashSet<String>(), descriptor);
            for (int i = 0; i <= size - 1; i++) {
                // 代表该索引没有被命中，跳过
                if (hitIndexNums[i] == 0) {
                    continue;
                }

                TableScanParam param = queryInfoWithIndexes.buildIndexTableQueryPrefix(i, hitIndexNums[i], descriptor);
                // 某次查询不合法说明不会
                if (!param.isValid()) {
                    return ResultUtil.getSuccessBaseResult();
                }

                /**
                 * 一个索引的查询可能有多次的scan 将其合并和再和原先的mergeResult求交集
                 */
                List<KeyPair> pairs = param.getKeyPairList();
                ListResult result = ResultUtil.getEmptyListResult();
                for (KeyPair pair : pairs) {
                    ListResult tmp = scanner.scan(ByteArrayUtils.getIndexTableName(tableName), pair.getStartKey(), pair.getEndKey());
                    result.union(tmp);
                }

                // 将一个索引命中的多个信息合并后再和原先的求交集
                filter.setIndex(existedIndex.get(i));
                filter.setHitNum(hitIndexNums[i]);
                mergedResult.merge(filter.filter(result));
                // 合并后结果为0，则直接返回list
                if (mergedResult.getResultSize() == 0) {
                    return ResultUtil.getSuccessBaseResult();
                }
            }
            // 未被索引命中的查询条件
            Set<String> unhitColumns = queryInfoWithIndexes.getUnhitColumns();

            // 说明不需要回表查询
            ListResult updateRowkeys;
            if (unhitColumns.size() == 0) {
                updateRowkeys = InternalResultUtils.buildResult(mergedResult.getMergedLineMap(), true);
            } else {
                LinkedHashMap<ByteBuffer, IndexLine> mergedMap = mergedResult.getMergedLineMap();
                // 构造回表查询的列，然后回表查询
                String[] backTableQualifiers = buildQualifiersForBackTable(unhitColumns, new HashSet<String>());

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
                    }

                    // 这里不需要merge新查询出来的行，因为delete的第一次查询只需要取rowkey
                }
                updateRowkeys = InternalResultUtils.buildResult(mergedMap, true);
            }
            // TODO: 2017-10-25 根据updateRowkey信息去删除数据表和索引表中对应的记录 

        }
        return ResultUtil.getSuccessBaseResult();
    }

    /**
     * 将结果转换为相应的形式
     *
     * @param mergedResult
     * @return
     */
    private AbstractResult buildResult(LinkedHashMap<ByteBuffer, IndexLine> mergedResult, boolean containsRowkey) {
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
    private String[] buildQualifiersForBackTable(Set<String> unhitColumns, Set<String> leftColumns) {
        Set<String> res = new HashSet<>();
        res.addAll(unhitColumns);
        res.addAll(leftColumns);
        return res.toArray(new String[]{});
    }
}
