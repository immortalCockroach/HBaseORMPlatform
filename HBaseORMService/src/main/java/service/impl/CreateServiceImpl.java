package service.impl;

import com.immortalcockroach.hbaseorm.api.CreateService;
import com.immortalcockroach.hbaseorm.entity.Column;
import com.immortalcockroach.hbaseorm.param.CreateIndexParam;
import com.immortalcockroach.hbaseorm.param.CreateTableParam;
import com.immortalcockroach.hbaseorm.result.BaseResult;
import com.immortalcockroach.hbaseorm.util.Bytes;
import com.immortalcockroach.hbaseorm.util.ResultUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.RegionSplitter;
import service.constants.ServiceConstants;
import service.enums.SplitAlgorithmsEnum;
import service.hbasemanager.creation.TableCreateService;
import service.hbasemanager.creation.TableIndexService;
import service.hbasemanager.utils.HBaseClusterUtils;
import service.hbasemanager.utils.HBaseTableUtils;
import service.utils.ByteArrayUtils;

import javax.annotation.Resource;

/**
 * create相关的操作，包括索引和表
 * Created by immortalCockroach on 8/30/17.
 */
public class CreateServiceImpl implements CreateService {
    @Resource
    private TableCreateService tableCreateService;

    @Resource
    private TableIndexService tableIndexService;

    @Override
    public BaseResult createTable(CreateTableParam createTableParam) {
        byte[] tableName = createTableParam.getTableName();
        // 检测表是否存在
        if (HBaseTableUtils.tableExists(tableName)) {
            return ResultUtil.getFailedBaseResult("表" + Bytes.toString(tableName) + "已经存在");
        }
        // 客户端已经验证过algorithm的正确性(UUID或者AutoIncrement或者null)，此处跳过验证
        Column[] columns = createTableParam.getColumns();
        String algorithmName = createTableParam.getSplitAlgorithms();
        if (!StringUtils.isBlank(algorithmName)) {
            byte[][] splitRange;
            // UUID切分
            int existClusterNum = HBaseClusterUtils.getExistClusterNum() * 2;
            int splitNum = createTableParam.getSplitNum() == null ? existClusterNum : createTableParam.getSplitNum();

            // UUID切分
            if (algorithmName.equals(SplitAlgorithmsEnum.UUID_SPLITTER.getAlgorithmName())) {
                splitRange = SplitAlgorithmsEnum.UUID_SPLITTER.getAlgorithm().split(splitNum);
            } else {
                RegionSplitter.SplitAlgorithm algorithm = SplitAlgorithmsEnum.AUTO_INCREMENT_SPLITTER.getAlgorithm();

                // 上下界的确定由client端的API完整值的设置
                Integer lowerBound = createTableParam.getLowerBound();
                Integer upperBound = createTableParam.getUpperBound();
                algorithm.setFirstRow(String.valueOf(lowerBound));
                algorithm.setLastRow(String.valueOf(upperBound));
                splitRange = algorithm.split(splitNum);
            }

            BaseResult result = tableCreateService.createTable(tableName, splitRange, columns);
            return result;
        } else {
            BaseResult result = tableCreateService.createTable(tableName, columns);
            return result;
        }
    }

    /**
     * 创建索引，当索引表不存在的时候创建索引表
     *
     * @param createIndexParam
     * @return
     */
    @Override
    public BaseResult createIndex(CreateIndexParam createIndexParam) {
        if (!ServiceConstants.USE_INDEX) {
            return ResultUtil.getFailedBaseResult("索引功能未开启");
        }
        byte[] tableName = createIndexParam.getTableName();
        if (!HBaseTableUtils.tableExists(tableName)) {
            return ResultUtil.getFailedBaseResult("数据表" + Bytes.toString(tableName) + "不存在");
        }

        byte[] indexTableName = ByteArrayUtils.getIndexTableName(tableName);
        // 检测表是否存在
        if (!HBaseTableUtils.tableExists(indexTableName)) {
            // 索引表不存在则创建索引表
            BaseResult createRes = tableCreateService.createTable(indexTableName);
            if (!createRes.getSuccess()) {
                // 如果创建索引表失败了就直接返回
                return ResultUtil.getFailedBaseResult("创建索引表失败，请稍后再试");
            }
        }
        // 创建索引
        BaseResult indexRes = tableIndexService.createIndex(tableName, indexTableName, createIndexParam.getQualifiers(), createIndexParam.getSize());

        return indexRes;

    }
}
