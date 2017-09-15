package service.hbasemanager.insert;

import com.immortalcockroach.hbaseorm.constant.CommonConstants;
import com.immortalcockroach.hbaseorm.result.BaseResult;
import com.immortalcockroach.hbaseorm.util.Bytes;
import com.immortalcockroach.hbaseorm.util.ResultUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;
import service.constants.ServiceConstants;
import service.hbasemanager.connection.HBaseConnectionPool;
import service.hbasemanager.read.TableScanService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by immortalCockroach on 8/31/17.
 */
@Service
public class TableInsertService {
    private static final Logger logger = Logger.getLogger(TableScanService.class);

    /**
     * 插入若干行
     *
     * @param tableName
     * @return
     */
    public BaseResult insertBatch(byte[] tableName, List<Map<String, byte[]>> valuesList) {

        Connection connection = HBaseConnectionPool.getConnection();

        try (Table table = connection.getTable(TableName.valueOf(tableName))) {

            int size = valuesList.size();
            int index = 0;

            while (size >= ServiceConstants.THRESHOLD) {
                List<Put> puts = getPutFromValuesList(valuesList, index, index + ServiceConstants.THRESHOLD - 1);
                table.put(puts);
                index += ServiceConstants.THRESHOLD;
                size -= ServiceConstants.THRESHOLD;
            }
            // 将末尾部分put
            if (size > 0) {
                List<Put> puts = getPutFromValuesList(valuesList, index, index + size - 1);
                table.put(puts);
            }


        } catch (IOException e) {
            logger.warn(e.getMessage(), e);
            return ResultUtil.getFailedBaseResult("写入error");
        }

        return ResultUtil.getSuccessBaseResult();
    }

    /**
     * 插入单行
     * @param tableName
     * @param values
     * @return
     */
    public BaseResult insert(byte[] tableName, Map<String, byte[]> values) {

        Connection connection = HBaseConnectionPool.getConnection();

        try (Table table = connection.getTable(TableName.valueOf(tableName))) {

            Put put = new Put(values.remove(CommonConstants.ROW_KEY));
            for (Map.Entry<String, byte[]> entry : values.entrySet()) {
                put.addColumn(ServiceConstants.BYTES_COLUMN_FAMILY, Bytes.toBytes(entry.getKey()), entry.getValue());
            }
            table.put(put);

        } catch (IOException e) {
            logger.warn(e.getMessage(), e);
            return ResultUtil.getFailedBaseResult("写入error");
        }

        return ResultUtil.getSuccessBaseResult();
    }

    /**
     * 将[start, stop]之间的数据转换为put
     *
     * @param valuesList
     * @param start
     * @param stop
     * @return
     */
    private List<Put> getPutFromValuesList(List<Map<String, byte[]>> valuesList, int start, int stop) {
        List<Put> res = new ArrayList<>(stop - start + 1);

        for (int i = start; i <= stop; i++) {
            Map<String, byte[]> lineMap = valuesList.get(i);
            // 首先获得rowkey
            Put p = new Put(lineMap.remove(CommonConstants.ROW_KEY));
            byte[] family = ServiceConstants.BYTES_COLUMN_FAMILY;
            for (Map.Entry<String, byte[]> entry : lineMap.entrySet()) {
                p.addColumn(family, Bytes.toBytes(entry.getKey()), entry.getValue());
            }
            res.add(p);
        }
        return res;
    }
}
