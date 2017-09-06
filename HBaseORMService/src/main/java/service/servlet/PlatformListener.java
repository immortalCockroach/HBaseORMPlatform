package service.servlet;


import org.apache.log4j.Logger;
import service.constants.ServiceConstants;
import service.hbasemanager.connection.HBaseConnectionPool;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * 用于启动的时候负责连接HBase和关闭的时候断开和HBase的连接，以及global_idx表的维护
 *
 * Created by immortalCockRoach on 2016/5/13.
 */
public class PlatformListener implements ServletContextListener {
    private static final Logger logger = Logger.getLogger(PlatformListener.class);

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        if (HBaseConnectionPool.getConnection() == null) {
            logger.fatal("connect to HBase failed");
        } else {
            logger.info("connect to HBase when established");
        }

    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        logger.info("disconnect to HBase when destroyed");
        HBaseConnectionPool.closeConnection();
    }
}
