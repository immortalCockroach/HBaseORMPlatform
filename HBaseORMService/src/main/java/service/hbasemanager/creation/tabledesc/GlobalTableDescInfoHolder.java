package service.hbasemanager.creation.tabledesc;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.immortalcockroach.hbaseorm.constant.CommonConstants;
import com.immortalcockroach.hbaseorm.entity.Column;
import com.immortalcockroach.hbaseorm.result.ListResult;
import com.immortalcockroach.hbaseorm.util.Bytes;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import service.constants.ServiceConstants;
import service.hbasemanager.entity.tabldesc.TableDescriptor;
import service.hbasemanager.read.TableScanService;

import javax.annotation.Resource;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 全局数据表信息的维护
 * global_desc表的结构为：
 * TableName——col1_1, col1_2,col1_3
 * Created by immortalCockroach on 9/26/17.
 */
public class GlobalTableDescInfoHolder {
    @Resource
    private TableScanService scanner;

    // 全局索引表，String为数据表名字
    private ConcurrentHashMap<String, TableDescriptor> globalTableDescMap;

    /**
     * Spring加载该类时维护所有数据表的表结构信息
     */
    public void init() {
        ListResult result = scanner.scan(ServiceConstants.GLOBAL_DESC_TABLE_BYTES, new String[]{ServiceConstants
                .GLOBAL_DESC_TABLE_COL});
        if (!result.getSuccess()) {
            return;
        }

        JSONArray rows = result.getData();
        int size = result.getSize();
        globalTableDescMap = new ConcurrentHashMap<>();
        for (int i = 0; i <= size - 1; i++) {
            // global_desc表的结构为tableName -
            JSONObject row = rows.getJSONObject(i);
            byte[] rowkey = row.getBytes(CommonConstants.ROW_KEY);
            String[] columns = Bytes.toString(row.getBytes
                    (ServiceConstants.GLOBAL_DESC_TABLE_COL)).split(ServiceConstants.GLOBAL_DESC_TABLE_SEPARATOR);

            globalTableDescMap.put(Bytes.toString(rowkey), new TableDescriptor(columns));
        }
    }

    /**
     * 创建表时在此处增加表的信息
     * 1、更新内存map的信息
     * 2、将对应的信息写入到global-desc中
     *
     * @param tableName
     * @param columns
     */
    public void addDescriptor(byte[] tableName, Column[] columns) {
        this.globalTableDescMap.put(Bytes.toString(tableName), new TableDescriptor(columns));
    }

    public TableDescriptor getDescriptor(byte[] tableName) {
        return this.globalTableDescMap.get(Bytes.toString(tableName));
    }

    /**
     * 根据表名和列名获得列的类型
     *
     * @param tableName
     * @param column
     * @return
     */
    public Integer getColumnType(byte[] tableName, String column) {
        if (ArrayUtils.isEmpty(tableName) || StringUtils.isBlank(column)) {
            return null;
        }
        TableDescriptor descriptor = globalTableDescMap.get(Bytes.toString(tableName));
        if (descriptor == null) {
            return null;
        }
        return descriptor.getTypeOfColumn(column);
    }
}
