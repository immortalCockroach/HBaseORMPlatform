package com.immortalcockroach.hbaseorm.param.condition;

import java.io.Serializable;
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

}
