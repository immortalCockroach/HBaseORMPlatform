package service.hbasemanager.deletion;

import com.immortalcockroach.hbaseorm.result.BaseResult;
import com.immortalcockroach.hbaseorm.util.ResultUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;
import service.constants.ServiceConstants;
import service.hbasemanager.connection.HBaseConnectionPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class TableDeleteService {
    private static final Logger logger = Logger.getLogger(TableDeleteService.class);

    public BaseResult deleteBatch(byte[] tableName, List<byte[]> rowkeyList) {
        if (rowkeyList == null || rowkeyList.size() == 0) {
            return ResultUtil.getSuccessBaseResult();
        }

        Connection connection = HBaseConnectionPool.getConnection();

        try (Table table = connection.getTable(TableName.valueOf(tableName))) {

            int size = rowkeyList.size();
            int index = 0;

            while (size >= ServiceConstants.THRESHOLD) {
                List<Delete> deletes = getDeleteFromRowkeyList(rowkeyList, index, index + ServiceConstants.THRESHOLD - 1);
                table.delete(deletes);
                index += ServiceConstants.THRESHOLD;
                size -= ServiceConstants.THRESHOLD;
            }
            // 将末尾部分put
            if (size > 0) {
                List<Delete> deletes = getDeleteFromRowkeyList(rowkeyList, index, index + size - 1);
                table.delete(deletes);
            }


        } catch (IOException e) {
            logger.warn(e.getMessage(), e);
            return ResultUtil.getFailedBaseResult("写入error");
        }

        return ResultUtil.getSuccessBaseResult();
    }

    private List<Delete> getDeleteFromRowkeyList(List<byte[]> rowkeyList, int start, int stop) {
        List<Delete> res = new ArrayList<>(stop - start + 1);

        for (int i = start; i <= stop; i++) {

            res.add(new Delete(rowkeyList.get(i)));

        }
        return res;
    }

}
