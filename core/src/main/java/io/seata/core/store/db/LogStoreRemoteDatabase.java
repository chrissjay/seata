package io.seata.core.store.db;

import io.seata.common.exception.StoreException;
import io.seata.core.store.BranchTransactionDO;
import io.seata.core.store.GlobalTransactionDO;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class LogStoreRemoteDatabase {

    public static final String URL = "jdbc:mysql://10.1.11.112:33009/microservice?useUnicode=true&characterEncoding=utf-8&useSSL=false";
    public static final String USER = "root";
    public static final String PASSWORD = "123456";

    public static void insertRemoteGlobalTransactionDO(GlobalTransactionDO globalTransactionDO){
        String sql = LogStoreSqls.getInsertGlobalTransactionSQL("seata_global_table", "mysql");
        PreparedStatement ps = null;
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            conn = DriverManager.getConnection(URL, USER, PASSWORD);
            conn.setAutoCommit(true);
            ps = conn.prepareStatement(sql);
            ps.setString(1, globalTransactionDO.getXid());
            ps.setLong(2, globalTransactionDO.getTransactionId());
            ps.setInt(3, globalTransactionDO.getStatus());
            ps.setString(4, globalTransactionDO.getApplicationId());
            ps.setString(5, globalTransactionDO.getTransactionServiceGroup());
            String transactionName = globalTransactionDO.getTransactionName();
            transactionName = transactionName.length() > 128 ? transactionName.substring(0, 128) : transactionName;
            ps.setString(6, transactionName);
            ps.setInt(7, globalTransactionDO.getTimeout());
            ps.setLong(8, globalTransactionDO.getBeginTime());
            ps.setString(9, globalTransactionDO.getApplicationData());
            ps.executeUpdate();
            return;
        } catch (SQLException e) {
            throw new StoreException(e);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                }
            }
        }
    }

    public static void insertRemoteBranchTransactionDO(BranchTransactionDO branchTransactionDO){
        String sql = LogStoreSqls.getInsertBranchTransactionSQL("seata_branch_table", "mysql");
        PreparedStatement ps = null;
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            conn = DriverManager.getConnection(URL, USER, PASSWORD);
            conn.setAutoCommit(true);
            ps = conn.prepareStatement(sql);
            ps.setString(1, branchTransactionDO.getXid());
            ps.setLong(2, branchTransactionDO.getTransactionId());
            ps.setLong(3, branchTransactionDO.getBranchId());
            ps.setString(4, branchTransactionDO.getResourceGroupId());
            ps.setString(5, branchTransactionDO.getResourceId());
            ps.setString(6, branchTransactionDO.getLockKey());
            ps.setString(7, branchTransactionDO.getBranchType());
            ps.setInt(8, branchTransactionDO.getStatus());
            ps.setString(9, branchTransactionDO.getClientId());
            ps.setString(10, branchTransactionDO.getApplicationData());
            ps.executeUpdate();
            return;
        } catch (SQLException e) {
            throw new StoreException(e);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                }
            }
        }
    }


}
