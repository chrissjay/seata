package io.seata.rm.datasource.undo.sybase;

import com.alibaba.druid.util.JdbcConstants;
import io.seata.common.exception.NotSupportYetException;
import io.seata.rm.datasource.undo.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.sql.*;

public class SybaseUndoLogManager extends AbstractUndoLogManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(SybaseUndoLogManager.class);

    private static String INSERT_UNDO_LOG_SQL = "INSERT INTO " + UNDO_LOG_TABLE_NAME + "\n" +
            "\t(id,branch_id, xid,context, rollback_info, log_status, log_created, log_modified)\n" +
            "VALUES (UNDO_LOG_SEQ.nextval,?, ?,?, ?, ?, sysdate, sysdate)";

    //TODO
    private static final String DELETE_UNDO_LOG_BY_CREATE_SQL = "DELETE FROM " + UNDO_LOG_TABLE_NAME +
            " WHERE log_created <= ? and ROWNUM <= ?";

    @Override
    public String getDbType() {
        return JdbcConstants.SYBASE;
    }

    private static void assertDbSupport(String dbType) {
        if (!JdbcConstants.ORACLE.equalsIgnoreCase(dbType)) {
            throw new NotSupportYetException("DbType[" + dbType + "] is not support yet!");
        }
    }

    @Override
    public int deleteUndoLogByLogCreated(java.util.Date logCreated, int limitRows, Connection conn) throws SQLException {
        PreparedStatement deletePST = null;
        try {
            deletePST = conn.prepareStatement(DELETE_UNDO_LOG_BY_CREATE_SQL);
            deletePST.setDate(1, new java.sql.Date(logCreated.getTime()));
            deletePST.setInt(2, limitRows);
            int deleteRows = deletePST.executeUpdate();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("batch delete undo log size " + deleteRows);
            }
            return deleteRows;
        } catch (Exception e) {
            if (!(e instanceof SQLException)) {
                e = new SQLException(e);
            }
            throw (SQLException) e;
        } finally {
            if (deletePST != null) {
                deletePST.close();
            }
        }
    }

    @Override
    protected void insertUndoLogWithNormal(String xid, long branchID, String rollbackCtx,
                                                byte[] undoLogContent, Connection conn) throws SQLException {
        insertUndoLog(xid, branchID,rollbackCtx, undoLogContent, State.Normal, conn);
    }

    @Override
    protected void insertUndoLogWithGlobalFinished(String xid, long branchID, UndoLogParser parser,
                                                        Connection conn) throws SQLException {
        insertUndoLog(xid, branchID, buildContext(parser.getName()),
                parser.getDefaultContent(), State.GlobalFinished, conn);
    }

    private static void insertUndoLog(String xid, long branchID, String rollbackCtx,
                                      byte[] undoLogContent, State state, Connection conn) throws SQLException {
        PreparedStatement pst = null;
        try {
            pst = conn.prepareStatement(INSERT_UNDO_LOG_SQL);
            pst.setLong(1, branchID);
            pst.setString(2, xid);
            pst.setString(3, rollbackCtx);
            ByteArrayInputStream inputStream = new ByteArrayInputStream(undoLogContent);
            pst.setBlob(4, inputStream);
            pst.setInt(5, state.getValue());
            pst.executeUpdate();
        } catch (Exception e) {
            if (!(e instanceof SQLException)) {
                e = new SQLException(e);
            }
            throw (SQLException) e;
        } finally {
            if (pst != null) {
                pst.close();
            }
        }
    }

}
