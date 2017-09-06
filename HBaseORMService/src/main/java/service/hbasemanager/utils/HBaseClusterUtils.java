package service.hbasemanager.utils;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.log4j.Logger;
import service.hbasemanager.connection.HBaseConnectionPool;

import java.io.IOException;

/**
 * 获得当前集群节点中的存活的节点的数量
 * Created by immortalCockroach on 8/30/17.
 */
public class HBaseClusterUtils {
    private static final Logger logger = Logger.getLogger(HBaseClusterUtils.class);

    public static int getExistClusterNum() {
        Connection connection = HBaseConnectionPool.getConnection();
        try (Admin admin = connection.getAdmin()) {

            return admin.getClusterStatus().getServersSize();
        } catch (IOException e) {
            logger.warn(e.getMessage(), e);
            return 0;
        }
    }
}
