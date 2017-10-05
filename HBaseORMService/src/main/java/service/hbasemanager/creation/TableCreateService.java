package service.hbasemanager.creation;


import com.immortalcockroach.hbaseorm.constant.CommonConstants;
import com.immortalcockroach.hbaseorm.entity.Column;
import com.immortalcockroach.hbaseorm.result.BaseResult;
import com.immortalcockroach.hbaseorm.util.Bytes;
import com.immortalcockroach.hbaseorm.util.ResultUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;
import service.constants.ServiceConstants;
import service.hbasemanager.connection.HBaseConnectionPool;
import service.hbasemanager.creation.tabledesc.GlobalTableDescInfoHolder;
import service.hbasemanager.insert.TableInsertService;
import service.hbasemanager.read.TableGetService;
import service.hbasemanager.read.TableScanService;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 创建HBase的表,目前的方法只有CreateTable,参数为表名
 * Created by immortalCockRoach on 2016/4/26.
 */
@Service
public class TableCreateService {

    private static final Logger logger = Logger.getLogger(TableCreateService.class);

    @Resource
    private TableInsertService inserter;

    @Resource
    private TableGetService getter;

    @Resource
    private TableScanService scanner;

    @Resource
    private GlobalTableDescInfoHolder descInfoHolder;

    /**
     * 创建单个表，没有列信息以及split信息，一般为索引表
     *
     * @param tableName
     * @return
     */
    public BaseResult createTable(byte[] tableName) {

        Connection connection = HBaseConnectionPool.getConnection();

        try (Admin admin = connection.getAdmin()) {

            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));

            tableDesc.addFamily(new HColumnDescriptor(ServiceConstants.BYTES_COLUMN_FAMILY));
            admin.createTable(tableDesc);
        } catch (IOException ioe) {
            logger.error(ioe);
            return ResultUtil.getFailedBaseResult("创建表失败");
        }

        return ResultUtil.getSuccessBaseResult();
    }


    public BaseResult createTable(byte[] tableName, Column[] columns) {

        Connection connection = HBaseConnectionPool.getConnection();

        try (Admin admin = connection.getAdmin()) {

            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));

            tableDesc.addFamily(new HColumnDescriptor(ServiceConstants.BYTES_COLUMN_FAMILY));
            admin.createTable(tableDesc);

            this.addTableDesc(tableName, columns);
        } catch (IOException ioe) {
            logger.error(ioe);
            return ResultUtil.getFailedBaseResult("创建表失败");
        }

        return ResultUtil.getSuccessBaseResult();
    }

    public BaseResult createTable(byte[] tableName, byte[][] splitRange, Column[] columns) {

        Connection connection = HBaseConnectionPool.getConnection();

        try (Admin admin = connection.getAdmin()) {

            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));

            tableDesc.addFamily(new HColumnDescriptor(ServiceConstants.BYTES_COLUMN_FAMILY));
            admin.createTable(tableDesc, splitRange);
            this.addTableDesc(tableName, columns);
        } catch (IOException ioe) {
            logger.error(ioe);
            return ResultUtil.getFailedBaseResult("创建表失败");
        }

        return ResultUtil.getSuccessBaseResult();
    }

    /**
     * 创建表时
     * 1、更新全局的descMap信息
     * 2、将columns信息写入到table中
     *
     * @param tableName
     * @param columns
     */
    private void addTableDesc(byte[] tableName, Column[] columns) {
        // 增加全局map的信息
        descInfoHolder.addDescriptor(tableName, columns);

        // 将表的列信息保存到global_desc表中
        String columnDesc = buildColumnsDescBytes(columns);
        Map<String, byte[]> lineMap = new HashMap<>();
        lineMap.put(CommonConstants.ROW_KEY, tableName);
        lineMap.put(ServiceConstants.GLOBAL_DESC_TABLE_COL, Bytes.toBytes(columnDesc));
        inserter.insert(ServiceConstants.GLOBAL_DESC_TABLE_BYTES, lineMap);
    }

    /**
     * 根据column的信息将其拼接为
     * col1_1,col2_2的形式，其中1和2为列的类型信息
     *
     * @param columns
     * @return
     */
    private String buildColumnsDescBytes(Column[] columns) {
        StringBuilder builder = new StringBuilder();
        for (Column column : columns) {
            // "col1_2,"的形式
            builder.append(column.getColumnName());
            builder.append(ServiceConstants.GLOBAL_DESC_TABLE_INNER_SEPARATOR);
            builder.append(String.valueOf(column.getType()));
            builder.append(ServiceConstants.GLOBAL_INDEX_TABLE_INDEX_INNER_SEPARATOR);
        }
        // 去掉最后的'_'
        return builder.substring(0, builder.length() - 1);
    }

}
