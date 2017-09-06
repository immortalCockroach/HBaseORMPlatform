package service.hbasemanager.utils;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.log4j.Logger;
import service.hbasemanager.connection.HBaseConnectionPool;

import java.io.IOException;

/**
 * 涉及到Hbase表的通用类
 * Created by immortalCockRoach on 2016/6/6.
 */
public class HBaseTableUtils {

    private static final Logger logger = Logger.getLogger(HBaseTableUtils.class);

    public static boolean tableExists(byte[] tableName) {
        Connection connection = HBaseConnectionPool.getConnection();
        try (Admin admin = connection.getAdmin()) {

            return admin.tableExists(TableName.valueOf(tableName));
        } catch (IOException e) {
            logger.warn(e.getMessage(), e);
            return false;
        }
    }
}
