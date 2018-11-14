package com.hisense.dao;

import java.sql.*;

public class OracleDaoOrigin {
    /**
     * @param ip       数据库地址
     * @param dbName   数据库库名
     * @param username 数据库账户
     * @param password 数据库密码
     * @param port     数据库连接
     * @return 返回数据库连接
     */
    public static Connection getConnection(String ip, String dbName, String username, String password, String port) {
        String portOri = "1521";
        Connection conn = null;
        if (port != null) {
            portOri = port;
        }
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            String url = "jdbc:oracle:thin:@//" + ip + ":" + portOri + "/" + dbName;
//			String username="jhgx";
//			String password="jhgx";
            conn = DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * @return 返回数据库连接
     */
    public static Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            String url = "jdbc:oracle:thin:@//192.168.0.108:1521/hiatmp";
            String username = "jhgx";
            String password = "jhgx";
            conn = DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static void free(ResultSet rs, PreparedStatement ps, Connection conn) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {

                try {
                    if (conn != null) {
                        conn.close();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }

        }

    }
}
