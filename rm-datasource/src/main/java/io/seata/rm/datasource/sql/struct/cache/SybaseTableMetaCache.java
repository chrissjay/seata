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
package io.seata.rm.datasource.sql.struct.cache;

import com.alibaba.druid.util.JdbcConstants;
import com.alibaba.druid.util.StringUtils;
import io.seata.rm.datasource.AbstractConnectionProxy;
import io.seata.rm.datasource.sql.struct.*;
import io.seata.rm.datasource.undo.KeywordChecker;
import io.seata.rm.datasource.undo.KeywordCheckerFactory;

import javax.sql.DataSource;
import java.sql.*;

public class SybaseTableMetaCache extends AbstractTableMetaCache {

    private static volatile TableMetaCache tableMetaCache = null;

    private SybaseTableMetaCache() {
    }

    /**
     * get instance of type MySQL keyword checker
     *
     * @return instance
     */
    public static TableMetaCache getInstance() {
        if (tableMetaCache == null) {
            synchronized (SybaseTableMetaCache.class) {
                if (tableMetaCache == null) {
                    tableMetaCache = new SybaseTableMetaCache();
                }
            }
        }
        return tableMetaCache;
    }

    @Override
    protected TableMeta fetchSchema(DataSource dataSource, String tableName) throws SQLException {
        return fetchSchemeInDefaultWay(dataSource, tableName);
    }

    private static TableMeta fetchSchemeInDefaultWay(DataSource dataSource, String tableName)
            throws SQLException {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = dataSource.getConnection();
            stmt = conn.createStatement();
            StringBuffer sb = new StringBuffer("SELECT TOP 1 * FROM " + tableName);
            rs = stmt.executeQuery(sb.toString());
            ResultSetMetaData rsmd = rs.getMetaData();
            DatabaseMetaData dbmd = conn.getMetaData();

            return resultSetMetaToSchema(rsmd, dbmd, tableName);
        } catch (Exception e) {
            if (e instanceof SQLException) {
                throw e;
            }
            throw new SQLException("Failed to fetch schema of " + tableName, e);

        } finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }

    private static TableMeta resultSetMetaToSchema(ResultSet rs2, AbstractConnectionProxy conn,
                                                   String tableName) throws SQLException {
        TableMeta tm = new TableMeta();
        tm.setTableName(tableName);
        while (rs2.next()) {
            ColumnMeta col = new ColumnMeta();
            col.setTableName(tableName);
            col.setColumnName(rs2.getString("COLUMN_NAME"));
            String datatype = rs2.getString("DATA_TYPE");
            if (StringUtils.equalsIgnoreCase(datatype, "NUMBER")) {
                col.setDataType(Types.BIGINT);
            } else if (StringUtils.equalsIgnoreCase(datatype, "VARCHAR2")) {
                col.setDataType(Types.VARCHAR);
            } else if (StringUtils.equalsIgnoreCase(datatype, "CHAR")) {
                col.setDataType(Types.CHAR);
            } else if (StringUtils.equalsIgnoreCase(datatype, "DATE")) {
                col.setDataType(Types.DATE);
            }

            col.setColumnSize(rs2.getInt("DATA_LENGTH"));

            tm.getAllColumns().put(col.getColumnName(), col);
        }

        Statement stmt = null;
        ResultSet rs1 = null;
        try {
            stmt = conn.getTargetConnection().createStatement();
            rs1 = stmt.executeQuery(
                    "select a.constraint_name,  a.column_name from user_cons_columns a, user_constraints b  where a"
                            + ".constraint_name = b.constraint_name and b.constraint_type = 'P' and a.table_name ='"
                            + tableName + "'");
            while (rs1.next()) {
                String indexName = rs1.getString(1);
                String colName = rs1.getString(2);
                ColumnMeta col = tm.getAllColumns().get(colName);

                if (tm.getAllIndexes().containsKey(indexName)) {
                    IndexMeta index = tm.getAllIndexes().get(indexName);
                    index.getValues().add(col);
                } else {
                    IndexMeta index = new IndexMeta();
                    index.setIndexName(indexName);
                    index.getValues().add(col);
                    index.setIndextype(IndexType.PRIMARY);
                    tm.getAllIndexes().put(indexName, index);

                }
            }
        } finally {
            if (rs1 != null) {
                rs1.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }

        return tm;
    }

    private static TableMeta resultSetMetaToSchema(ResultSetMetaData rsmd, DatabaseMetaData dbmd, String tableName)
            throws SQLException {
        String schemaName = rsmd.getSchemaName(1);
        String catalogName = rsmd.getCatalogName(1);

        TableMeta tm = new TableMeta();
        tm.setTableName(tableName);

        ResultSet rs1 = dbmd.getColumns(catalogName, schemaName, tableName, "%");
        while (rs1.next()) {
            ColumnMeta col = new ColumnMeta();
            col.setTableCat(rs1.getString("TABLE_CAT"));
            col.setTableSchemaName(rs1.getString("TABLE_SCHEM"));
            col.setTableName(rs1.getString("TABLE_NAME"));
            col.setColumnName(rs1.getString("COLUMN_NAME"));
            col.setDataType(rs1.getInt("DATA_TYPE"));
            col.setDataTypeName(rs1.getString("TYPE_NAME"));
            col.setColumnSize(rs1.getInt("COLUMN_SIZE"));
            col.setDecimalDigits(rs1.getInt("DECIMAL_DIGITS"));
            col.setNumPrecRadix(rs1.getInt("NUM_PREC_RADIX"));
            col.setNullAble(rs1.getInt("NULLABLE"));
            col.setRemarks(rs1.getString("REMARKS"));
            col.setColumnDef(rs1.getString("COLUMN_DEF"));
            col.setSqlDataType(rs1.getInt("SQL_DATA_TYPE"));
            col.setSqlDatetimeSub(rs1.getInt("SQL_DATETIME_SUB"));
            col.setCharOctetLength(rs1.getInt("CHAR_OCTET_LENGTH"));
            col.setOrdinalPosition(rs1.getInt("ORDINAL_POSITION"));
            col.setIsNullAble(rs1.getString("IS_NULLABLE"));
            // todo 问题
//            col.setIsAutoincrement(rs1.getString("IS_AUTOINCREMENT"));

            tm.getAllColumns().put(col.getColumnName(), col);
        }

        ResultSet rs2 = dbmd.getIndexInfo(catalogName, schemaName, tableName, false, true);
        String indexName = "";
        while (rs2.next()) {
            indexName = rs2.getString("INDEX_NAME");
            String colName = rs2.getString("COLUMN_NAME");
            ColumnMeta col = tm.getAllColumns().get(colName);
            if(org.apache.commons.lang.StringUtils.isBlank(indexName)){
                continue;
            }
            if (tm.getAllIndexes().containsKey(indexName)) {
                IndexMeta index = tm.getAllIndexes().get(indexName);
                index.getValues().add(col);
            } else {
                IndexMeta index = new IndexMeta();
                index.setIndexName(indexName);
                index.setNonUnique(rs2.getBoolean("NON_UNIQUE"));
                index.setIndexQualifier(rs2.getString("INDEX_QUALIFIER"));
                index.setIndexName(rs2.getString("INDEX_NAME"));
                index.setType(rs2.getShort("TYPE"));
                index.setOrdinalPosition(rs2.getShort("ORDINAL_POSITION"));
                index.setAscOrDesc(rs2.getString("ASC_OR_DESC"));
                index.setCardinality(rs2.getInt("CARDINALITY"));
                index.getValues().add(col);

                if ("PRIMARY".equalsIgnoreCase(indexName) || indexName.equalsIgnoreCase(rsmd.getTableName(1) + "_pk")) {
                    index.setIndextype(IndexType.PRIMARY);
                } else if (!index.isNonUnique()) {
                    index.setIndextype(IndexType.Unique);
                } else {
                    index.setIndextype(IndexType.Normal);
                }
                tm.getAllIndexes().put(indexName, index);

            }
        }
        IndexMeta index = tm.getAllIndexes().get(indexName);
        if (index.getIndextype().value() != 0) {
            if ("H2 JDBC Driver".equals(dbmd.getDriverName())) {
                if (indexName.length() > 11 && "PRIMARY_KEY".equalsIgnoreCase(indexName.substring(0, 11))) {
                    index.setIndextype(IndexType.PRIMARY);
                }
            } else if (dbmd.getDriverName() != null && dbmd.getDriverName().toLowerCase().indexOf("postgresql") >= 0) {
                if ((tableName + "_pk").equalsIgnoreCase(indexName)) {
                    index.setIndextype(IndexType.PRIMARY);
                }
            }
        }
        return tm;
    }

}
