/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.rm.datasource.sql.druid.sybase;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;
import io.seata.rm.datasource.ParametersHolder;
import io.seata.rm.datasource.sql.SQLParsingException;
import io.seata.rm.datasource.sql.SQLType;
import io.seata.rm.datasource.sql.SQLUpdateRecognizer;
import io.seata.rm.datasource.sql.druid.BaseRecognizer;

import java.util.ArrayList;
import java.util.List;

/**
 * The type My sql update recognizer.
 *
 * @author sharajava
 */
public class SybaseUpdateRecognizer extends BaseRecognizer implements SQLUpdateRecognizer {

    private SQLUpdateStatement ast;

    /**
     * Instantiates a new My sql update recognizer.
     *
     * @param originalSQL the original sql
     * @param ast         the ast
     */
    public SybaseUpdateRecognizer(String originalSQL, SQLStatement ast) {
        super(originalSQL);
        this.ast = (SQLUpdateStatement)ast;
    }

    @Override
    public SQLType getSQLType() {
        return SQLType.UPDATE;
    }

    @Override
    public List<String> getUpdateColumns() {
        List<SQLUpdateSetItem> updateSetItems = ast.getItems();
        List<String> list = new ArrayList<>(updateSetItems.size());
        for (SQLUpdateSetItem updateSetItem : updateSetItems) {
            SQLExpr expr = updateSetItem.getColumn();
            if (expr instanceof SQLIdentifierExpr) {
                list.add(((SQLIdentifierExpr)expr).getName());
            } else if (expr instanceof SQLPropertyExpr) {
                // This is alias case, like UPDATE xxx_tbl a SET a.name = ? WHERE a.id = ?
                SQLExpr owner = ((SQLPropertyExpr)expr).getOwner();
                if (owner instanceof SQLIdentifierExpr) {
                    list.add((((SQLIdentifierExpr)owner).getName() + "." + ((SQLPropertyExpr)expr).getName()));
                }
            } else {
                throw new SQLParsingException("Unknown SQLExpr: " + expr.getClass() + " " + expr);
            }
        }
        return list;
    }

    @Override
    public List<Object> getUpdateValues() {
        List<SQLUpdateSetItem> updateSetItems = ast.getItems();
        List<Object> list = new ArrayList<>(updateSetItems.size());
        for (SQLUpdateSetItem updateSetItem : updateSetItems) {
            SQLExpr expr = updateSetItem.getValue();
            if (expr instanceof SQLValuableExpr) {
                list.add(((SQLValuableExpr)expr).getValue());
            } else if (expr instanceof SQLVariantRefExpr) {
                list.add(new VMarker());
            } else {
                throw new SQLParsingException("Unknown SQLExpr: " + expr.getClass() + " " + expr);
            }
        }
        return list;
    }

    @Override
    public String getWhereCondition(final ParametersHolder parametersHolder, final ArrayList<List<Object>> paramAppenderList) {
        SQLExpr where = ast.getWhere();
        if (where == null) {
            return "";
        }
        StringBuffer sb = new StringBuffer();
        SQLASTOutputVisitor visitor = new SQLASTOutputVisitor(sb) {
            @Override
            public boolean visit(SQLVariantRefExpr x) {
                if ("?".equals(x.getName())) {
                    ArrayList<Object> oneParamValues = parametersHolder.getParameters()[x.getIndex()];
                    if (paramAppenderList.size() == 0) {
                        oneParamValues.stream().forEach(t -> paramAppenderList.add(new ArrayList<>()));
                    }
                    for (int i = 0; i < oneParamValues.size(); i++) {
                        paramAppenderList.get(i).add(oneParamValues.get(i));
                    }

                }
                return super.visit(x);
            }
        };
        if (where instanceof SQLBinaryOpExpr) {
            visitor.visit((SQLBinaryOpExpr)where);
        } else if (where instanceof SQLInListExpr) {
            visitor.visit((SQLInListExpr) where);
        } else if (where instanceof SQLBetweenExpr) {
            visitor.visit((SQLBetweenExpr) where);
        } else {
            throw new IllegalArgumentException("unexpected WHERE expr: " + where.getClass().getSimpleName());
        }
        return sb.toString();
    }

    @Override
    public String getWhereCondition() {
        SQLExpr where = ast.getWhere();
        if (where == null) {
            return "";
        }

        StringBuffer sb = new StringBuffer();
        SQLASTOutputVisitor visitor = new SQLASTOutputVisitor(sb);

        if (where instanceof SQLBetweenExpr) {
            visitor.visit((SQLBetweenExpr)where);
        } else if (where instanceof SQLInListExpr) {
            visitor.visit((SQLInListExpr)where);
        } else {
            visitor.visit((SQLBinaryOpExpr)where);
        }

        return sb.toString();
    }

    @Override
    public String getTableAlias() {
        return ast.getTableSource().getAlias();
    }

    @Override
    public String getTableName() {
        StringBuffer sb = new StringBuffer();
        SQLASTOutputVisitor visitor = new SQLASTOutputVisitor(sb) {

            @Override
            public boolean visit(SQLExprTableSource x) {
                printTableSourceExpr(x.getExpr());
                return false;
            }
        };
        SQLExprTableSource tableSource = (SQLExprTableSource)ast.getTableSource();
        visitor.visit(tableSource);
        return sb.toString();
    }

}