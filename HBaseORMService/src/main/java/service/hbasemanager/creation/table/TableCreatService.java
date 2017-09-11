package service.hbasemanager.creation.table;


import com.immortalcockroach.hbaseorm.result.BaseResult;
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

import java.io.IOException;

/**
 * 创建HBase的表,目前的方法只有CreateTable,参数为表名
 * Created by immortalCockRoach on 2016/4/26.
 */
@Service
public class TableCreatService {

    private static final Logger logger = Logger.getLogger(TableCreatService.class);

    /**
     * 根据TableName建表
     * 当参数错误，连接HBase失败，表已经存在或者发生其他IO错误时都会抛出异常，由上层负责处理异常并返回给用户结果
     *
     * @param tableName
     * @throws IOException
     */
    public BaseResult createTable(byte[] tableName, byte[][] splitRange) {

        Connection connection = HBaseConnectionPool.getConnection();

        try (Admin admin = connection.getAdmin()) {

            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));

            tableDesc.addFamily(new HColumnDescriptor(ServiceConstants.BYTES_COLUMN_FAMILY));
            admin.createTable(tableDesc, splitRange);


        } catch (IOException ioe) {
            logger.error(ioe);
            return ResultUtil.getFailedBaseResult("创建表失败");
        }

        return ResultUtil.getSuccessBaseResult();
    }

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

}
