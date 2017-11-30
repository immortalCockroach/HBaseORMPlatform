package service.hbasemanager.entity.index;

import service.constants.ServiceConstants;

/**
 * 代表一个table的单个索引的信息（可能是联合索引）
 * Created by immortalCockroach on 9/1/17.
 */
public class Index {
    // 构成这个索引的列信息，和索引构建时列的顺序相同
    private String[] indexColumnList;

    private int[] size;
    // 索引在表中的序号
    private byte indexNum;

    // 索引的最大字节数
    private int indexLength;

    // 整体的索引，用于判断索引是否存在
    private String indexString;

    public Index(String indexString, byte indexNum) {
        this.indexString = indexString;
        this.indexNum = indexNum;

        String[] tmp = indexString.split(ServiceConstants.GLOBAL_INDEX_TABLE_INDEX_INNER_SEPARATOR);
        int length = tmp.length;
        this.indexLength = 0;
        size = new int[length / 2];
        indexColumnList = new String[length / 2];
        for (int i = 0; i <= tmp.length - 1; i += 2) {
            indexColumnList[i / 2] = tmp[i];

            int l = Integer.valueOf(tmp[i + 1]);
            size[i / 2] = l;
            this.indexLength += l;
        }
    }

    public String[] getIndexColumnList() {
        return indexColumnList;
    }

    public void setIndexColumnList(String[] indexColumnList) {
        this.indexColumnList = indexColumnList;
    }

    public byte getIndexNum() {
        return indexNum;
    }

    public void setIndexNum(byte indexNum) {
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

    public int[] getSize() {
        return size;
    }

    public int getIndexLength() {
        return indexLength;
    }

    public String getIndexString() {
        return indexString;
    }
}
