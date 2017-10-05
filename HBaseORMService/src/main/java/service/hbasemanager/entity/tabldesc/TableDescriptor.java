package service.hbasemanager.entity.tabldesc;

import com.immortalcockroach.hbaseorm.entity.Column;

import java.util.HashMap;
import java.util.Map;

/**
 * 代表表的结构信息
 * Created by immortalCockroach on 10/4/17.
 */
public class TableDescriptor {
    private Map<String, Integer> descriptor;

    public TableDescriptor(Column[] columns) {
        descriptor = new HashMap<>();
        for (Column column : columns) {
            descriptor.put(column.getColumnName(), column.getType());
        }
    }

    public TableDescriptor(String[] columns) {
        descriptor = new HashMap<>();

        for (String str : columns) {
            String[] column = str.split("_");
            descriptor.put(column[0], Integer.valueOf(column[1]));
        }
    }

    public Integer getTypeOfColumn(String column) {
        return descriptor.get(column);
    }
}
