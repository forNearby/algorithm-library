package com.hisense.dao;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author machao
 * @date 2018年10月30日
 */
public class DaoUtil {
    private OracleDao oracleDao = new OracleDao();

    /**
     * @param dbtable 从数据库查的表
     * @return 返回数据库内容
     */
    public Dataset<Row> getTable(String dbtable) {
        return oracleDao.daoAdapt(dbtable);
    }
    
    /**
     * 重载
     */
    public Dataset<Row> getTable(String dbtable,String master,String url,String user,String password) {
        return oracleDao.daoAdapt(dbtable,master,url,user,password);
    }

    /**
     * @param dbtable 从数据库查的表
     * @param dbView  放到内存中的表名字
     * @param sql     从内存中操作表
     * @return 返回数据库内容
     */
    public Dataset<Row> operateBySql(String dbtable, String dbView, String sql) {
        return oracleDao.daoAdapt(dbtable, dbView, sql);
    }

    /**
     * @param dbtable      从数据库查的表
     * @param ip           服务器IP地址
     * @param port         网络端口
     * @param dataBaseName 数据库名
     * @return 返回数据库内容
     */
    public Dataset<Row> getTableByDataBaseInfo(String dbtable, String ip, String port, String dataBaseName, String user, String password) {
        return oracleDao.daoAdaptByDataBaseInfo(dbtable, ip, port, dataBaseName, user, password);
    }

    public Dataset<Row> getTableByDataBaseInfo(String dbtable, String ip, String port, String dataBaseName, String user, String password, String master) {
        return oracleDao.daoAdaptByDataBaseInfo(dbtable, ip, port, dataBaseName, user, password, master);
    }

    /**
     * @param dbtable      从数据库查的表
     * @param dbView       放到内存中的表明
     * @param sql          从内存中操作表
     * @param ip           服务器IP地址
     * @param port         网络端口
     * @param dataBaseName 数据库名
     * @param user         用户名
     * @param password     密码
     * @return 返回数据库内容
     */
    public Dataset<Row> operateByDataBaseInfo(String dbtable, String dbView, String sql, String ip, String port, String dataBaseName, String user, String password, String master) {
        return oracleDao.daoAdaptByDataBaseInfo(dbtable, dbView, sql, ip, port, dataBaseName, user, password, master);
    }

    public int update(String sql, String ip, String dbName, String username, String password, String port) {
        Connection conn = null;
        PreparedStatement ps = null;
        int rs = 0;
        try {
            conn = OracleDaoOrigin.getConnection(ip, dbName, username, password, port);
            ps = conn.prepareStatement(sql);
            rs = ps.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            OracleDaoOrigin.free(null, ps, conn);
        }
        return rs;
    }

    public int update(String sql) {
        Connection conn = null;
        PreparedStatement ps = null;
        int rs = 0;
        try {
            conn = OracleDaoOrigin.getConnection();
            ps = conn.prepareStatement(sql);
            rs = ps.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            OracleDaoOrigin.free(null, ps, conn);
        }
        return rs;
    }
}
