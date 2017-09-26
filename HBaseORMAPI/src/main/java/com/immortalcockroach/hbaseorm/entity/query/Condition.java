package com.immortalcockroach.hbaseorm.entity.query;

import com.immortalcockroach.hbaseorm.param.enums.LogicalOperatorEnum;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Condition implements Serializable {

    private static final long serialVersionUID = 4334372213720809054L;

    private List<Expression> expressions;
    private List<Integer> logicOperators;

    public Condition() {

    }


    public List<Expression> getExpressions() {
        return expressions;
    }

    public void setExpressions(List<Expression> expressions) {
        this.expressions = expressions;
    }

    public List<Integer> getLogicOperators() {
        return logicOperators;
    }

    public void setLogicOperators(List<Integer> logicOperators) {
        this.logicOperators = logicOperators;
    }

    public Condition add(Expression expression) {
        expressions.add(expression);
        if (expressions.size() > 1) {
            logicOperators.add(LogicalOperatorEnum.AND.getId());
        }
        return this;
    }

    public List<String> getQueryColumns() {
        List<String> columns = new ArrayList<>();
        for (Expression expression : expressions) {
            columns.add(expression.getColumn());
        }
        return columns;
    }

}
