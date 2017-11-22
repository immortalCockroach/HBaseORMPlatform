package service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.immortalcockroach.hbaseorm.api.DeleteService;
import com.immortalcockroach.hbaseorm.constant.CommonConstants;
import com.immortalcockroach.hbaseorm.entity.query.Expression;
import com.immortalcockroach.hbaseorm.param.DeleteParam;
import com.immortalcockroach.hbaseorm.result.BaseResult;
import com.immortalcockroach.hbaseorm.result.ListResult;
import com.immortalcockroach.hbaseorm.util.Bytes;
import com.immortalcockroach.hbaseorm.util.ResultUtil;
import org.apache.hadoop.hbase.filter.Filter;
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
import service.utils.FilterUtils;
import service.utils.IndexUtils;
import service.utils.InternalResultUtils;

import javax.annotation.Resource;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;

public class DeleteServiceImpl implements DeleteService {

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
        ListResult deleteRows;
        if (!IndexUtils.hitAnyIdex(hitIndexNums)) {
            Filter filter = FilterUtils.buildFilterListWithCondition(deleteParam.getCondition(), descriptor);
            Expression rokweyExpression = deleteParam.getCondition().getRowKeyExp();
            if (rokweyExpression == null) {
                deleteRows = scanner.scan(tableName, new String[]{}, filter);
            } else {
                deleteRows = scanner.scan(tableName, rokweyExpression.getValue(), rokweyExpression.getOptionValue(),
                        new String[]{}, filter);
            }
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

            LinkedHashMap<ByteBuffer, IndexLine> mergedMap = mergedResult.getMergedLineMap();
            byte[][] rowkeys = ByteArrayUtils.getRowkeys(mergedMap.keySet());

            ListResult backTableRes = getter.readSeparatorLines(tableName, rowkeys, new String[]{});
            // 获得完整的需要删除的行
            deleteRows = InternalResultUtils.buildResult(mergedMap, backTableRes, filter, false);
        }
        if (deleteRows.getSize() > 0) {
            deleter.deleteBatch(tableName, buildDataTableRowKey(deleteRows));
            deleter.deleteBatch(ByteArrayUtils.getIndexTableName(tableName), IndexUtils.buildIndexTableRowKey(deleteRows, indexInfoHolder.getTableIndexes(tableName)));
        }

        return ResultUtil.getSuccessBaseResult();
    }


    private List<byte[]> buildDataTableRowKey(ListResult deleteRows) {
        int size = deleteRows.getSize();
        List<byte[]> rowkeys = new ArrayList<>(size);
        JSONArray array = deleteRows.getData();

        for (int i = 0; i <= size - 1; i++) {
            JSONObject row = array.getJSONObject(i);
            rowkeys.add(row.getBytes(CommonConstants.ROW_KEY));
        }
        return rowkeys;
    }


}
