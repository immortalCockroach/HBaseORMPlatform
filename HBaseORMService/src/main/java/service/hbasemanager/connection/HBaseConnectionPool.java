package service.hbasemanager.connection;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 管理HBase连接的类
 * 由于创建{@link Connection}是一个重量级的操作，因此对HBase的操作的时候不要每次都重新连接，而是使用已有的连接
 * 目前该类中使用单例模式维护一个对HBase的长连接
 * 使用方法为
 * <pre>
 * Connection connection = HBaseConnectionPool.getConnection();
 * Table table = connection.getTable(TableName.valueOf("mytable"));
 * try {
 *   table.get(...);
 *   ...
 * } finally {
 *   table.close();
 * }
 * </pre>
 * 连接的关闭操作放到该平台关闭的时候
 * Created by immortalCockRoach on 2016/4/8.
 */
public class HBaseConnectionPool {

    private static final Logger logger = Logger.getLogger(HBaseConnectionPool.class);

    /**
     * 返回HBase的连接实例,线程安全
     *
     * @return HBase的连接实例
     */
    public static final Connection getConnection() {
        return HBaseConnectionHolder.singletonConnection;
    }

    /**
     * 关闭平台的时候调用该方法释放长连接
     */
    public static final void closeConnection() {
        if (HBaseConnectionHolder.singletonConnection != null) {
            try {
                HBaseConnectionHolder.singletonConnection.close();
                logger.info("close connection to HBase");
            } catch (IOException e) {
                logger.error("close fail:" + e);
            }
        }
    }

    private static class HBaseConnectionHolder {
        private static Connection singletonConnection = null;

        static {
            try {
                singletonConnection = ConnectionFactory.createConnection();
            } catch (IOException e) {
                singletonConnection = null;
                logger.fatal("can't connect to HBase:" + e);
            }
        }
    }


}
