package service.hbasemanager.entity.scanparam;

import com.immortalcockroach.hbaseorm.entity.query.Expression;

import java.util.List;
import java.util.Map;

/**
 * 用于构建范围查询时索引表前缀的查询startKey和endkey
 * Created by immortalCockroach on 10/5/17.
 */
public class IndexParam {
    private Map<String, byte[]> linePrefix;
    private List<String> qualifiers;
    // 代表范围查询的条件，包含列名和表达式
    private Expression expression;
    private byte indexNum;

    public IndexParam(Map<String, byte[]> linePrefix, List<String> qualifiers, Expression expression, byte indexNum) {
        this.linePrefix = linePrefix;
        this.qualifiers = qualifiers;
        this.expression = expression;
        this.indexNum = indexNum;
    }

    public Map<String, byte[]> getLinePrefix() {

        return linePrefix;
    }

    public void setLinePrefix(Map<String, byte[]> linePrefix) {
        this.linePrefix = linePrefix;
    }

    public List<String> getQualifiers() {
        return qualifiers;
    }

    public void setQualifiers(List<String> qualifiers) {
        this.qualifiers = qualifiers;
    }

    public Expression getExpression() {
        return expression;
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    public byte getIndexNum() {
        return indexNum;
    }

    public void setIndexNum(byte indexNum) {
        this.indexNum = indexNum;
    }

    public void addLinePrefix(String column, byte[] value) {
        this.linePrefix.put(column, value);
    }

    public void addQualifier(String qualifier) {
        this.qualifiers.add(qualifier);
    }
}
