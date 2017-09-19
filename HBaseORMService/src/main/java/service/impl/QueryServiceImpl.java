package service.impl;

import com.immortalcockroach.hbaseorm.api.QueryService;
import com.immortalcockroach.hbaseorm.param.QueryParam;
import com.immortalcockroach.hbaseorm.result.AbstractResult;
import com.immortalcockroach.hbaseorm.result.ListResult;
import com.immortalcockroach.hbaseorm.util.Bytes;
import com.immortalcockroach.hbaseorm.util.ResultUtil;
import service.hbasemanager.creation.index.GlobalIndexInfoHolder;
import service.hbasemanager.entity.filter.IndexLineFilter;
import service.hbasemanager.entity.index.Index;
import service.hbasemanager.entity.index.QueryInfoWithIndexes;
import service.hbasemanager.entity.indexresult.IndexScanResult;
import service.hbasemanager.entity.indexresult.MergedResult;
import service.hbasemanager.read.TableGetService;
import service.hbasemanager.read.TableScanService;
import service.hbasemanager.utils.HBaseTableUtils;
import service.utils.ByteArrayUtils;
import service.utils.IndexUtils;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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

        } else {
            QueryInfoWithIndexes queryInfoWithIndexes = new QueryInfoWithIndexes(existedIndex, queryParam.getCondition().getExpressions(), hitIndexNums);
            // size代表该表的索引数量
            int size = hitIndexNums.length;
            MergedResult mergedResult = new MergedResult(qualifiers);
            for (int i = 0; i <= size - 1; i++) {
                // 代表该索引没有被命中，跳过
                if (hitIndexNums[i] == 0) {
                    continue;
                }
                // startKey
                byte[] startKey = queryInfoWithIndexes.buildIndexTableQueryPrefix(i, hitIndexNums[i]);
                byte[] endKey = new byte[]{(byte) (existedIndex.get(i).getIndexNum() + 1)};
                ListResult result = scanner.scan(ByteArrayUtils.getIndexTableName(tableName), startKey, endKey);
                if (!result.getSuccess() || result.getSize() == 0) {
                    return ResultUtil.getEmptyListResult();
                }
                IndexLineFilter filter = new IndexLineFilter(existedIndex.get(i), hitIndexNums[i], queryInfoWithIndexes.getExpressionMap(), qualifiers);
                IndexScanResult res = filter.filter(result);
                mergedResult.merge(res);
                // 合并后结果为0，则直接返回list
                if (mergedResult.getResultSize() == 0) {
                    return ResultUtil.getEmptyListResult();
                }

            }


        }

        return null;

/*        byte[] startKey = queryParam.getStartKey();
        byte[] endKey = queryParam.getEndKey();
        // 如果key有null的情况，或者两个key不等。则为范围查询
        if (ArrayUtils.isEmpty(startKey) || ArrayUtils.isEmpty(endKey) || !ArrayUtils.isEquals(startKey, endKey)) {
            // endKey为null说明读取到末尾，直接scan
            if (ArrayUtils.isEmpty(endKey)) {
                return scanner.scan(queryParam.getTableName(), startKey,
                        endKey, queryParam.getQualifiers());
            } else {
                // scan会忽略lastRow 此时分为2个读取
                PlainResult lastRow = getter.read(queryParam.getTableName(), queryParam.getStartKey(), queryParam.getQualifiers());
                // 如果是有startKey和endKey的 则endKey需要单独查询一次
                ListResult res = scanner.scan(queryParam.getTableName(), startKey,
                        endKey, queryParam.getQualifiers());

                // 最后一行存在则加入结果集
                if (lastRow.getSize() == 1) {
                    res.getData().add(lastRow);
                    res.setSize(res.getSize() + 1);
                }
                return res;
            }
        } else {
            // 2个key相等，单个查询
            return getter.read(queryParam.getTableName(), queryParam.getStartKey(), queryParam.getQualifiers());
        }*/

    }
}
