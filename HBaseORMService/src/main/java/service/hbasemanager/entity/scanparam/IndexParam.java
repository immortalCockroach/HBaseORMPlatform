package service.hbasemanager.entity.scanparam;

import com.immortalcockroach.hbaseorm.entity.query.Expression;
import service.hbasemanager.entity.index.Index;

import java.util.Map;

/**
 * 用于构建范围查询时索引表前缀的查询startKey和endkey
 * Created by immortalCockroach on 10/5/17.
 */
public class IndexParam {
    private Map<String, byte[]> linePrefix;
    // 代表范围查询的条件，包含列名和表达式
    private Expression expression;
    private Index index;
    private int hitNum;

    public IndexParam(Map<String, byte[]> linePrefix, int hitNum, Expression expression, Index index) {
        this.linePrefix = linePrefix;
        this.hitNum = hitNum;
        this.expression = expression;
        this.index = index;
    }

    public Map<String, byte[]> getLinePrefix() {

        return linePrefix;
    }

    public void setLinePrefix(Map<String, byte[]> linePrefix) {
        this.linePrefix = linePrefix;
    }

    public int getHitNum() {
        return hitNum;
    }

    public void setHitNum(int hitNum) {
        this.hitNum = hitNum;
    }

    public Expression getExpression() {
        return expression;
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    public Index getIndex() {
        return index;
    }

    public void setIndex(Index indexNum) {
        this.index = indexNum;
    }

    public void addOrUpdateLinePrefix(String column, byte[] value) {
        this.linePrefix.put(column, value);
    }

}
