package service.hbasemanager.entity.index;

import service.constants.ServiceConstants;

import java.util.Arrays;
import java.util.List;

/**
 * 代表一个table的单个索引的信息（可能是联合索引）
 * Created by immortalCockroach on 9/1/17.
 */
public class Index {
    // 构成这个索引的列信息，和索引构建时列的顺序相同
    private List<String> indexColumnList;
    // 索引在表中的序号
    private int indexNum;

    // 整体的索引，用于判断索引是否存在
    private String indexString;

    public Index(String indexString, int indexNum) {
        this.indexString = indexString;
        this.indexNum = indexNum;
        this.indexColumnList = Arrays.asList(indexString.split(ServiceConstants.GLOBAL_INDEX_TABLE_INDEX_INNER_SEPARATOR));
    }

    public List<String> getIndexColumnList() {
        return indexColumnList;
    }

    public void setIndexColumnList(List<String> indexColumnList) {
        this.indexColumnList = indexColumnList;
    }

    public int getIndexNum() {
        return indexNum;
    }

    public void setIndexNum(int indexNum) {
        this.indexNum = indexNum;
    }

    /**
     * 判断新建的索引是否和当前索引重复，判断是否为当前索引的前缀即可
     *
     * @param newIndex
     * @return
     */
    public boolean duplicate(String newIndex) {
        return indexString.startsWith(newIndex);
    }
}
