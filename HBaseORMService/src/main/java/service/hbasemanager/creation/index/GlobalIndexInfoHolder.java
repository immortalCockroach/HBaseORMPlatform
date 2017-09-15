package service.hbasemanager.creation.index;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.immortalcockroach.hbaseorm.constant.CommonConstants;
import com.immortalcockroach.hbaseorm.result.ListResult;
import com.immortalcockroach.hbaseorm.util.Bytes;
import service.constants.ServiceConstants;
import service.hbasemanager.entity.Index;
import service.hbasemanager.insert.TableInsertService;
import service.hbasemanager.read.TableGetService;
import service.hbasemanager.read.TableScanService;
import service.utils.IndexUtils;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 全局索引表的维护
 * 全局索引表的信息为
 * tableName——col1_col2_col3,col4(一个表的索引数量不能超过128)
 * 内存map的信息和上面类似，只是索引的信息放在Set中
 * Created by immortalCockroach on 9/1/17.
 */
public class GlobalIndexInfoHolder {
    @Resource
    private TableScanService scanner;

    @Resource
    private TableInsertService inserter;

    @Resource
    private TableGetService getter;

    // 全局索引表，String为数据表名字
    private ConcurrentHashMap<String, List<Index>> globalIndexMap;

    /**
     * Spring注入后使用该方法加载数据表的索引信息
     */
    public void init() {
        if (ServiceConstants.USE_INDEX) {
            ListResult result = scanner.scan(ServiceConstants.GLOBAL_INDEX_TABLE_BYTES, new String[]{ServiceConstants.GLOBAL_INDEX_TABLE_COL});
            if (!result.getSuccess()) {
                return;
            }

            JSONArray rows = result.getData();
            int size = result.getSize();
            globalIndexMap = new ConcurrentHashMap<>();
            for (int i = 0; i <= size - 1; i++) {
                // global_idx表的结构为tableName - index
                JSONObject row = rows.getJSONObject(i);
                byte[] rowkey = row.getBytes(CommonConstants.ROW_KEY);
                String[] indexesColumns = Bytes.toString(row.getBytes
                        (ServiceConstants.GLOBAL_INDEX_TABLE_COL)).split(ServiceConstants.GLOBAL_INDEX_TABLE_INDEX_SEPARATOR);

                List<Index> indexes = new ArrayList<>(indexesColumns.length);
                for (int j = 0; j <= indexesColumns.length; j++) {
                    indexes.add(new Index(indexesColumns[j], j));
                }

                globalIndexMap.put(Bytes.toString(rowkey), indexes);
            }
        }
    }

    /**
     * 判断索引信息是否存在，前缀匹配也算是存在
     *
     * @param qualifiers
     * @return 代表当前索引的序号, 从0开始，为了多级索引查询的便利首部， -1代表已经存在
     */
    public int indexExists(byte[] tableName, String[] qualifiers) {
        String index = IndexUtils.getCombinedIndex(qualifiers);
        List<Index> indexes = globalIndexMap.get(Bytes.toString(tableName));
        if (indexes == null) {
            return 0;
        }

        // 如果当前索引为某个联合索引的前缀 则也算存在
        for (Index existedIndex : indexes) {
            if (existedIndex.duplicate(index)) {
                return -1;
            }
        }

        return indexes.size() > ServiceConstants.MAX_TABLE_INDEX_COUNT ? -1 : indexes.size();
    }

    /**
     * 更新globalMap中tableName对应的表的索引信息
     * @param tableName
     * @param qualifiers
     */
    public void updateGlobalMap(byte[] tableName, String[] qualifiers) {
        // 更新index当前global_idx表维护的索引信息
        String name = Bytes.toString(tableName);
        List<Index> indexes = globalIndexMap.get(name);
        if (indexes == null) {
            indexes = new ArrayList<>();
            globalIndexMap.put(name, indexes);
        }

        indexes.add(new Index(IndexUtils.getCombinedIndex(qualifiers), indexes.size()));
    }


    /**
     * 获得一个表的所有索引信息
     *
     * @param tableName 数据表的表名
     * @return
     */
    public List<Index> getTableIndexes(byte[] tableName) {
        return globalIndexMap.get(Bytes.toString(tableName));
    }


}
