package service.hbasemanager.entity;

import java.util.List;

/**
 * 代表一个索引被部分或者全部命中
 * 由于hitNum目前没有用上，暂时deprecated
 */
@Deprecated
public class HitIndex {
    // 构成这个索引的列信息，和索引构建时列的顺序相同
    private List<String> indexColumnList;
    // 索引在表中的序号
    private int indexNum;

    // 表示命中的索引前缀数
    private int hitNum;

    public HitIndex(List<String> indexColumnList, int indexNum, int hitNum) {
        this.indexColumnList = indexColumnList;
        this.indexNum = indexNum;
        this.hitNum = hitNum;
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

    public int getHitNum() {
        return hitNum;
    }

    public void setHitNum(int hitNum) {
        this.hitNum = hitNum;
    }
}
