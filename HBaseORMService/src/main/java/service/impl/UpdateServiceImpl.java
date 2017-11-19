package service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.immortalcockroach.hbaseorm.api.UpdateService;
import com.immortalcockroach.hbaseorm.constant.CommonConstants;
import com.immortalcockroach.hbaseorm.param.UpdateParam;
import com.immortalcockroach.hbaseorm.result.BaseResult;
import com.immortalcockroach.hbaseorm.result.ListResult;
import com.immortalcockroach.hbaseorm.result.PlainResult;
import com.immortalcockroach.hbaseorm.util.Bytes;
import com.immortalcockroach.hbaseorm.util.ResultUtil;
import org.apache.hadoop.hbase.filter.Filter;
import service.constants.ServiceConstants;
import service.hbasemanager.creation.TableIndexService;
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
import service.hbasemanager.insert.TableInsertService;
import service.hbasemanager.read.TableGetService;
import service.hbasemanager.read.TableScanService;
import service.hbasemanager.utils.HBaseTableUtils;
import service.utils.ByteArrayUtils;
import service.utils.FilterUtils;
import service.utils.IndexUtils;
import service.utils.InternalResultUtils;

import javax.annotation.Resource;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UpdateServiceImpl implements UpdateService {
    @Resource
    private TableInsertService inserter;

    @Resource
    private TableDeleteService deleter;

    @Resource
    private TableScanService scanner;

    @Resource
    private TableGetService getter;

    @Resource
    private GlobalIndexInfoHolder indexInfoHolder;

    @Resource
    private GlobalTableDescInfoHolder descInfoHolder;

    @Resource
    private TableIndexService tableIndexService;

    @Override
    public BaseResult update(UpdateParam updateParam) {
        byte[] tableName = updateParam.getTableName();
        // 检测表是否存在
        if (!HBaseTableUtils.tableExists(tableName)) {
            return ResultUtil.getFailedBaseResult("表" + Bytes.toString(tableName) + "不存在");
        }

        // 存在的索引信息
        List<Index> existedIndex = indexInfoHolder.getTableIndexes(tableName);

        TableDescriptor descriptor = descInfoHolder.getDescriptor(tableName);

        // 获得每个索引的命中信息
        int[] hitIndexNums = IndexUtils.getHitIndexWhenQuery(existedIndex, updateParam.getConditionColumnsType());
        // 直接全表扫描
        if (!IndexUtils.hitAnyIdex(hitIndexNums)) {
            Filter filter = FilterUtils.buildFilterListWithCondition(updateParam.getCondition(), descriptor);
            ListResult tmp = scanner.scan(tableName, new String[]{CommonConstants.ROW_KEY}, filter);
            return inserter.insertBatch(tableName, buildDataTablePuts(tmp, updateParam.getUpdateValues()));
        } else {
            QueryInfoWithIndexes queryInfoWithIndexes = new QueryInfoWithIndexes(existedIndex,
                    updateParam.getCondition().getExpressions(), hitIndexNums);
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
            ListResult updatedRows;
            if (unhitColumns.size() == 0) {
                updatedRows = InternalResultUtils.buildResult(mergedResult.getMergedLineMap(), true);
            } else {
                LinkedHashMap<ByteBuffer, IndexLine> mergedMap = mergedResult.getMergedLineMap();
                // 构造回表查询的列，然后回表查询
                String[] backTableQualifiers = InternalResultUtils.buildQualifiersForBackTable(unhitColumns, new HashSet<String>());

                Iterator<Map.Entry<ByteBuffer, IndexLine>> iterator = mergedMap.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<ByteBuffer, IndexLine> entry = iterator.next();
                    // 如果不在合并的结果里，则remove
                    ByteBuffer rowkey = entry.getKey();
                    // 此处update时需要读取所有的列，用于构建后续的删除行键与更新行键
                    PlainResult backTableLine = getter.read(tableName, rowkey.array(), new String[]{});
                    if (!backTableLine.getSuccess() || backTableLine.getSize() == 0) {
                        iterator.remove();
                        continue;
                    }
                    // 验证回表查询的结果，然后和当前的line合并
                    JSONObject line = backTableLine.getData();
                    // 此处不移除列，后面要建立索引表的更新与删除的行键
                    if (!filter.check(line, false)) {
                        iterator.remove();
                        continue;
                    }


                }
                // 将最后符合的行构建出完整的数据表的行
                updatedRows = InternalResultUtils.buildResult(mergedMap, true);
            }

            Map<String, byte[]> updateValues = updateParam.getUpdateValues();

            String[] updateQualifiers = updateValues.keySet().toArray(new String[]{});
            // 1. 根据更新的列筛选出命中的索引，删除索引
            // 2. 更新索引表数据
            List<Index> hitIndexes = tableIndexService.getHitIndexesWhenUpdate(tableName, updateQualifiers);
            if (hitIndexes.size() > 0) {
                byte[] indexTable = ByteArrayUtils.getIndexTableName(tableName);
                deleter.deleteBatch(indexTable, IndexUtils.buildIndexTableRowKey(updatedRows, hitIndexes));
                inserter.insertBatch(indexTable, buildIndexTablePuts(updatedRows, updateValues, hitIndexes));

            }

            // 3. 更新表数据
            List<Map<String, byte[]>> newPuts = buildDataTablePuts(updatedRows, updateValues);
            inserter.insertBatch(tableName, newPuts);
            return ResultUtil.getSuccessBaseResult();
        }

    }

    private List<Map<String, byte[]>> buildDataTablePuts(ListResult updatedRows, Map<String, byte[]> updatedValues) {
        int size = updatedRows.getSize();
        List<Map<String, byte[]>> newLines = new ArrayList<>(size);
        JSONArray array = updatedRows.getData();

        for (int i = 0; i <= size - 1; i++) {
            JSONObject row = array.getJSONObject(i);
            Map<String, byte[]> newRow = new HashMap<>(updatedValues);
            newRow.put(CommonConstants.ROW_KEY, row.getBytes(CommonConstants.ROW_KEY));
            newLines.add(newRow);
        }
        return newLines;
    }

    private List<Map<String, byte[]>> buildIndexTablePuts(ListResult updatedRows, Map<String, byte[]> updatedValues, List<Index> hitIndexes) {
        int size = updatedRows.getSize();
        List<Map<String, byte[]>> rowkeys = new ArrayList<>(size * hitIndexes.size());
        JSONArray array = updatedRows.getData();

        for (int i = 0; i <= size - 1; i++) {
            // 针对每一行，构建所有索引的索引表行健
            JSONObject row = array.getJSONObject(i);
            for (Index index : hitIndexes) {
                String[] qualifiers = index.getIndexColumnList().toArray(new String[]{});
                // 根据更新的值更新原始的行
                for (Map.Entry<String, byte[]> entry : updatedValues.entrySet()) {
                    row.put(entry.getKey(), entry.getValue());
                }
                // 构建索引表的行键
                Map<String, byte[]> lineMap = new HashMap<>(2);
                lineMap.put(CommonConstants.ROW_KEY, ByteArrayUtils.generateIndexRowKey(row, qualifiers, (byte) index.getIndexNum()));
                // 由于put的数据必须有列，所以此处指定一个，后续不使用col数据
                lineMap.put(ServiceConstants.GLOBAL_INDEX_TABLE_COL, Bytes.toBytes("1"));
                rowkeys.add(lineMap);
            }
        }
        return rowkeys;
    }

}
