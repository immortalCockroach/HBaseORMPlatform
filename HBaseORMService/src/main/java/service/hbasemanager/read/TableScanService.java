package service.hbasemanager.read;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.immortalcockroach.hbaseorm.constant.CommonConstants;
import com.immortalcockroach.hbaseorm.result.ListResult;
import com.immortalcockroach.hbaseorm.util.ResultUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;
import service.constants.ServiceConstants;
import service.hbasemanager.connection.HBaseConnectionPool;

import java.io.IOException;
import java.util.List;

/**
 * 读取表的多行记录
 * Created by immortalCockRoach on 2016/6/8.
 */
@Service
public class TableScanService {
    private static final Logger logger = Logger.getLogger(TableScanService.class);

    /**
     * 查询若干行
     *
     * @param tableName
     * @param startRow
     * @param stopRow
     * @param prefix
     * @param columnQualifiers
     * @return
     */
    public ListResult scan(byte[] tableName, byte[] startRow, byte[] stopRow, byte[] prefix, String[] columnQualifiers) {

        Connection connection = HBaseConnectionPool.getConnection();

        JSONArray res = new JSONArray();
        ResultScanner rScanner = null;
        byte[] family = ServiceConstants.BYTES_COLUMN_FAMILY;
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Scan scan = new Scan();
            if (startRow != null) {
                scan.setStartRow(startRow);
            }
            if (stopRow != null) {
                scan.setStopRow(stopRow);
            }
            if (prefix != null) {
                scan.setFilter(new PrefixFilter(prefix));
            }

            for (String s : columnQualifiers) {
                scan.addColumn(family, Bytes.toBytes(s));
            }

            scan.setCaching(50);
            scan.setBatch(10);
            // 此处直接listCells,null的话表示rowkey不存在或者查找的family:qualifer没有值，或者qualifier不存在
            rScanner = table.getScanner(scan);

            for (Result r = rScanner.next(); r != null; r = rScanner.next()) {
                List<Cell> list = r.listCells();
                JSONObject lineResult = new JSONObject();

                // null表示没有rowkey对应的行
                if (list != null) {
                    for (Cell c : list) {
                        lineResult.put(Bytes.toString(CellUtil.cloneQualifier(c)), CellUtil.cloneValue(c));
                    }
                    // 将行标识rowkey也加入到result中 key为常量"rowkey";
                    lineResult.put(CommonConstants.ROW_KEY, r.getRow());
                    // 将查询不到的列填充为null，有可能索引处需要使用
                    for (String q : columnQualifiers) {
                        if (!lineResult.containsKey(q)) {
                            lineResult.put(q, null);
                        }
                    }
                    res.add(lineResult);
                }

            }
        } catch (IOException e) {
            logger.warn(e.getMessage(), e);
            return ResultUtil.getFailedListResult("读取error");
        } finally {
            // ResultScanner记得要关闭
            if (rScanner != null) {
                rScanner.close();
            }
        }

        return ResultUtil.getSuccessListResult(res);
    }

    public ListResult scan(byte[] tableName) {
        return this.scan(tableName, null, null, null, null);
    }

    public ListResult scan(byte[] tableName, String[] qualifers) {
        return this.scan(tableName, null, null, null, qualifers);
    }

    public ListResult scan(byte[] tableName, byte[] startKey, byte[] endKey, String[] qualifers) {
        return this.scan(tableName, startKey, endKey, null, qualifers);
    }
}
