package service.impl;

import com.alibaba.fastjson.JSONObject;
import com.immortalcockroach.hbaseorm.api.QueryService;
import com.immortalcockroach.hbaseorm.constant.CommonConstants;
import com.immortalcockroach.hbaseorm.param.QueryParam;
import com.immortalcockroach.hbaseorm.result.ListResult;
import com.immortalcockroach.hbaseorm.result.PlainResult;
import com.immortalcockroach.hbaseorm.util.Bytes;
import com.immortalcockroach.hbaseorm.util.ResultUtil;
import service.hbasemanager.creation.index.GlobalIndexInfoHolder;
import service.hbasemanager.creation.tabledesc.GlobalTableDescInfoHolder;
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

    @Resource
    private GlobalTableDescInfoHolder descInfoHolder;

    @Override
    public ListResult query(QueryParam queryParam) {
        byte[] tableName = queryParam.getTableName();
        // 检测表是否存在
        if (!HBaseTableUtils.tableExists(tableName)) {
            return ResultUtil.getFailedListResult("表" + Bytes.toString(tableName) + "不存在");
        }

        // 存在的索引信息
        List<Index> existedIndex = indexInfoHolder.getTableIndexes(tableName);

        TableDescriptor descriptor = descInfoHolder.getDescriptor(tableName);

        Set<String> qualifiers = new HashSet<>(Arrays.asList(queryParam.getQualifiers()));

        // 获得每个索引的命中信息
        int[] hitIndexNums = IndexUtils.getHitIndexWhenQuery(existedIndex, queryParam.getConditionColumnsType());
        // 直接全表扫描
        if (!IndexUtils.hitAnyIdex(hitIndexNums)) {
            return null;
        } else {
            QueryInfoWithIndexes queryInfoWithIndexes = new QueryInfoWithIndexes(existedIndex, queryParam.getCondition().getExpressions(), hitIndexNums);
            // size代表该表的索引数量
            int size = hitIndexNums.length;
            MergedResult mergedResult = new MergedResult(qualifiers);
            IndexLineFilter filter = new IndexLineFilter(queryInfoWithIndexes.getExpressionMap(), qualifiers, descriptor);
            for (int i = 0; i <= size - 1; i++) {
                // 代表该索引没有被命中，跳过
                if (hitIndexNums[i] == 0) {
                    continue;
                }

                TableScanParam param = queryInfoWithIndexes.buildIndexTableQueryPrefix(i, hitIndexNums[i], descriptor);
                // 某次查询不合法说明不会
                if (!param.isValid()) {
                    return ResultUtil.getEmptyListResult();
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
                    return ResultUtil.getEmptyListResult();
                }
            }
            // 未被索引命中的查询条件
            Set<String> unhitColumns = queryInfoWithIndexes.getUnhitColumns();
            // 不在索引列中的筛选列
            Set<String> leftColumns = mergedResult.getLeftColumns();
            // 说明不需要回表查询
            if (unhitColumns.size() == 0 && leftColumns.size() == 0) {
                return InternalResultUtils.buildResult(mergedResult.getMergedLineMap(), qualifiers.contains(CommonConstants.ROW_KEY));
            } else {
                LinkedHashMap<ByteBuffer, IndexLine> mergedMap = mergedResult.getMergedLineMap();
                // 构造回表查询的列，然后回表查询
                String[] backTableQualifiers = InternalResultUtils.buildQualifiersForBackTable(unhitColumns, leftColumns);

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
                    if (!filter.check(line, true)) {
                        iterator.remove();
                        continue;
                    } else {
                        // 将2个结果merge
                        IndexLine oldLine = entry.getValue();
                        oldLine.mergeLine(line);
                    }
                }
                return InternalResultUtils.buildResult(mergedMap, qualifiers.contains(CommonConstants.ROW_KEY));
            }
        }
    }

}
