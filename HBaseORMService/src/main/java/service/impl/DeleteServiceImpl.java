package service.impl;

import com.immortalcockroach.hbaseorm.api.DeleteService;
import com.immortalcockroach.hbaseorm.param.DeleteParam;
import com.immortalcockroach.hbaseorm.result.BaseResult;
import com.immortalcockroach.hbaseorm.util.Bytes;
import com.immortalcockroach.hbaseorm.util.ResultUtil;
import service.hbasemanager.deletion.TableDeleteService;
import service.hbasemanager.utils.HBaseTableUtils;

import javax.annotation.Resource;

public class DeleteServiceImpl implements DeleteService {

    @Resource
    private TableDeleteService tableDeleteService;

    @Override
    public BaseResult delete(DeleteParam param) {
        byte[] tableName = param.getTableName();
        // 检测表是否存在
        if (!HBaseTableUtils.tableExists(tableName)) {
            return ResultUtil.getFailedBaseResult("表" + Bytes.toString(tableName) + "不存在");
        }

        return tableDeleteService.deleteBatch(param.getTableName(), param.getRowkeysList());
    }
}
