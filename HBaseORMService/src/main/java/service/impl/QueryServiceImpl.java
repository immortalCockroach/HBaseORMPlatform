package service.impl;

import com.immortalcockroach.hbaseorm.api.QueryService;
import com.immortalcockroach.hbaseorm.param.QueryParam;
import com.immortalcockroach.hbaseorm.param.condition.Expression;
import com.immortalcockroach.hbaseorm.result.AbstractResult;
import com.immortalcockroach.hbaseorm.result.ListResult;
import com.immortalcockroach.hbaseorm.util.Bytes;
import com.immortalcockroach.hbaseorm.util.ResultUtil;
import service.hbasemanager.creation.index.GlobalIndexInfoHolder;
import service.hbasemanager.entity.HitIndexes;
import service.hbasemanager.entity.Index;
import service.hbasemanager.read.TableGetService;
import service.hbasemanager.read.TableScanService;
import service.hbasemanager.utils.HBaseTableUtils;
import service.utils.ByteArrayUtils;
import service.utils.IndexUtils;

import javax.annotation.Resource;
import java.util.List;

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
        // 查询的表达式
        List<Expression> queryExpressions = queryParam.getCondition().getExpressions();

        // 获得每个索引的命中信息
        int[] hitIndexNums = IndexUtils.getHitIndexWhenQuery(existedIndex, queryParam.getQueryColumns());
        // 直接全表扫描
        if (!IndexUtils.hitAny(hitIndexNums)) {

        } else {
            HitIndexes hitIndexes = new HitIndexes(existedIndex, queryExpressions, hitIndexNums, queryParam.getQualifiers());
            // size代表该表的索引数量
            int size = hitIndexNums.length;

            for (int i = 0; i <= size - 1; i++) {
                // 代表该索引没有被命中，跳过
                if (hitIndexNums[i] == 0) {
                    continue;
                }
                byte[] prefix = hitIndexes.buildIndexTableQueryPrefix(i, hitIndexNums[i]);

                ListResult result = scanner.scan(ByteArrayUtils.getIndexTableName(tableName), prefix);
                if (!result.getSuccess() || result.getSize() == 0) {
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
