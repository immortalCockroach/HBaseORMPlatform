package service.hbasemanager.read;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.immortalcockroach.hbaseorm.constant.CommonConstants;
import com.immortalcockroach.hbaseorm.result.ListResult;
import com.immortalcockroach.hbaseorm.result.PlainResult;
import com.immortalcockroach.hbaseorm.util.ResultUtil;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;
import service.constants.ServiceConstants;
import service.hbasemanager.connection.HBaseConnectionPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * 读取表的某一行记录
 * Created by immortalCockRoach on 2016/5/12.
 */
@Service
public class TableGetService {

    private static final Logger logger = Logger.getLogger(TableGetService.class);

    /**
     * 查询表，如果表的一行为null，则返回空结果
     *
     * @return
     */
    public PlainResult read(byte[] tableName, byte[] rowKey, String[] columnQualifiers) {

        Connection connection = HBaseConnectionPool.getConnection();

        byte[] family = ServiceConstants.BYTES_COLUMN_FAMILY;
        JSONObject result = new JSONObject();
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Get g = new Get(rowKey);
            if (ArrayUtils.isEmpty(columnQualifiers)) {
                g.addFamily(family);
            } else {
                for (String s : columnQualifiers) {
                    g.addColumn(family, Bytes.toBytes(s));
                }
            }


            // 此处直接listCells,null的话表示rowkey不存在或者查找的family:qualifer没有值，或者qualifier不存在
            Result r = table.get(g);
            List<Cell> list = r.listCells();

            // null表示查询的qualifers都是null
            if (list != null) {
                for (Cell c : list) {
                    result.put(Bytes.toString(CellUtil.cloneQualifier(c)), CellUtil.cloneValue(c));
                }
                result.put(CommonConstants.ROW_KEY, r.getRow());
            } else {
                return ResultUtil.getEmptyPlainResult();
            }


        } catch (IOException e) {
            logger.warn(e.getMessage(), e);
            ResultUtil.getFailedPlainResult("读取失败");
        }

        return ResultUtil.getSuccessPlainResult(result);
    }

    public ListResult readSeparatorLines(byte[] tableName, byte[][] rowKeys, String[] columnQualifiers) {

        Connection connection = HBaseConnectionPool.getConnection();

        byte[] family = ServiceConstants.BYTES_COLUMN_FAMILY;
        JSONArray result = new JSONArray();
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            int size = rowKeys.length;
            List<Get> gets = new ArrayList<>(size);
            for (byte[] rowkey : rowKeys) {
                Get g = new Get(rowkey);
                if (ArrayUtils.isEmpty(columnQualifiers)) {
                    g.addFamily(family);
                } else {
                    for (String s : columnQualifiers) {
                        g.addColumn(family, Bytes.toBytes(s));
                    }
                }

            }

            // 此处直接listCells,null的话表示rowkey不存在或者查找的family:qualifer没有值，或者qualifier不存在
            Result[] res = table.get(gets);
            for (Result r : res) {
                JSONObject line = new JSONObject();
                List<Cell> list = r.listCells();

                // null表示查询的qualifers都是null
                if (list != null) {
                    for (Cell c : list) {
                        line.put(Bytes.toString(CellUtil.cloneQualifier(c)), CellUtil.cloneValue(c));
                    }
                    line.put(CommonConstants.ROW_KEY, r.getRow());
                    result.add(line);
                }
            }

        } catch (IOException e) {
            logger.warn(e.getMessage(), e);
            ResultUtil.getFailedPlainResult("读取失败");
        }

        return ResultUtil.getSuccessListResult(result);
    }
}
