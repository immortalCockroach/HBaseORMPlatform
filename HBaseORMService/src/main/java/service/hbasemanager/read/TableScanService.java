package service.hbasemanager.read;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.immortalcockroach.hbaseorm.constant.CommonConstants;
import com.immortalcockroach.hbaseorm.result.ListResult;
import com.immortalcockroach.hbaseorm.util.ResultUtil;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
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
     * 需要注意的是，由于当查询的qualifiers不存在的时候，HBase会过滤掉该行
     * 此处的解决方案是，qualifers用于查询结果的过滤，但是不传给Scan作为原生API的列过滤操作
     *
     * @param tableName
     * @param startRow
     * @param stopRow
     * @param columnQualifiers
     * @return
     */
    public ListResult scan(byte[] tableName, byte[] startRow, byte[] stopRow, String[] columnQualifiers, Filter filter) {

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
            if (filter != null) {
                scan.setFilter(filter);
            }

            // 此处只设置列族，qualifiers用于查询结果的手动过滤
            if (ArrayUtils.isEmpty(columnQualifiers)) {
                scan.addFamily(family);
            } else {
                for (String s : columnQualifiers) {
                    scan.addColumn(family, Bytes.toBytes(s));
                }
            }

            // 此处直接listCells,null的话表示rowkey不存在或者查找的family:qualifer没有值，或者qualifier不存在
            rScanner = table.getScanner(scan);

            for (Result r = rScanner.next(); r != null; r = rScanner.next()) {
                List<Cell> list = r.listCells();
                JSONObject lineResult = new JSONObject();

                // null表示没有rowkey对应的行
                if (list != null) {
                    // 如果列修饰符为null，则填充所有的列
                    for (Cell c : list) {
                        lineResult.put(Bytes.toString(CellUtil.cloneQualifier(c)), CellUtil.cloneValue(c));
                    }
                    lineResult.put(CommonConstants.ROW_KEY, r.getRow());
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

    public ListResult scan(byte[] tableName, byte[] startKey, byte[] endKey) {
        return this.scan(tableName, startKey, endKey, null, null);
    }

    public ListResult scan(byte[] tableName, String[] qualifers) {
        return this.scan(tableName, null, null, qualifers, null);
    }

    public ListResult scan(byte[] tableName, String[] qualifers, Filter filter) {
        return this.scan(tableName, null, null, qualifers, filter);
    }
}
