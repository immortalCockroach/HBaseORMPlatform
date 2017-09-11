package service.impl;

import com.immortalcockroach.hbaseorm.api.InsertService;
import com.immortalcockroach.hbaseorm.param.InsertParam;
import com.immortalcockroach.hbaseorm.result.BaseResult;
import com.immortalcockroach.hbaseorm.util.Bytes;
import com.immortalcockroach.hbaseorm.util.ResultUtil;
import service.hbasemanager.creation.index.GlobalIndexInfoHolder;
import service.hbasemanager.creation.index.TableIndexService;
import service.hbasemanager.insert.TableInsertService;
import service.hbasemanager.utils.HBaseTableUtils;

import javax.annotation.Resource;
import java.util.List;

/**
 * Created by immortalCockroach on 8/31/17.
 */
public class InsertServiceImpl implements InsertService {

    @Resource
    private TableInsertService tableInsertService;

    @Resource
    private GlobalIndexInfoHolder indexInfoHolder;

    @Resource
    private TableIndexService tableIndexService;

    /**
     * insert直接调用API就可以 不用处理参数
     *
     * @param insertParam
     * @return
     */
    @Override
    public BaseResult insert(InsertParam insertParam) {
        byte[] tableName = insertParam.getTableName();
        // 检测表是否存在
        if (!HBaseTableUtils.tableExists(tableName)) {
            return ResultUtil.getFailedBaseResult("表" + Bytes.toString(tableName) + "不存在");
        }
        BaseResult res = tableInsertService.insertBatch(insertParam.getTableName(),
                insertParam.getValuesMap());

        if (!res.getSuccess()) {
            return ResultUtil.getFailedBaseResult("插入数据错误，请稍后重试");
        }

        List<String> hitIndexes = indexInfoHolder.getHitIndexesWithinQualifiers(tableName, insertParam
                .getQualifiers());
        // 代表有索引命中了
        if (hitIndexes != null && hitIndexes.size() > 0) {
            BaseResult updateRes = tableIndexService.updateIndexWhenInsert(tableName, insertParam.getValuesMap(), hitIndexes);
            if (!updateRes.getSuccess()) {
                return ResultUtil.getFailedBaseResult("更新索引错误");
            }
        }

        return ResultUtil.getSuccessBaseResult();
    }
}
