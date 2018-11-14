package com.hisense.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author machao
 * @date 2018年10月30日
 */
public class VehicleDaoUtil {
	private String account;
	private String databaseAddress;
	private String user;
	private String password;
	private String tableName;
	private String port;
	private String master;
    private String resultAccount;
    private String resultDatabaseAddress;
    private String resultUser;
    private  String resultPassword;
    private String resultTablename;
    private String outPutPort;

	public void  setDataBase(DataBaseEntity dataBaseEntity) 
	{
		account = dataBaseEntity.getAccount();
		databaseAddress = dataBaseEntity.getDatabaseAddress();
		user = dataBaseEntity.getUser();
		password = dataBaseEntity.getPassword();
		tableName = dataBaseEntity.getTableName();
		master=dataBaseEntity.getMaster();
		resultAccount=dataBaseEntity.getOutPutAccount();
		resultDatabaseAddress=dataBaseEntity.getOutPutDatabaseAddress();
		resultUser=dataBaseEntity.getOutPutUser();
		resultPassword =dataBaseEntity.getOutPutPassword();
		resultTablename=dataBaseEntity.getOutPutTablename();
		port=dataBaseEntity.getPort();
		outPutPort=dataBaseEntity.getOutPutPort();
		
	}

	private OracleDao oracleDao = new OracleDao();

	/**
	 * @param dbtable
	 *            从数据库查的表
	 * @return 返回数据库内容
	 */
	public Dataset<Row> getTable(String dbtable) {
		return oracleDao.daoAdapt(dbtable);
	}

	/**
	 * @param dbtable
	 *            从数据库查的表
	 * @param dbView
	 *            放到内存中的表名字
	 * @param sql
	 *            从内存中操作表
	 * @return 返回数据库内容
	 */
	public Dataset<Row> operateBySql(String dbtable, String dbView, String sql) {
		return oracleDao.daoAdapt(dbtable, dbView, sql);
	}

	/**
	 * @param dbtable
	 *            从数据库查的表
	 * @param databaseAddress
	 *            服务器databaseAddress地址
	 * @param port
	 *            网络端口
	 * @param account
	 *            数据库名
	 * @return 返回数据库内容
	 */
	public Dataset<Row> getTableByDataBaseInfo(String dbtable) {
		return oracleDao.daoAdaptByDataBaseInfo(dbtable, databaseAddress, port, account, user, password);
	}

	/**
	 * @param dbtable
	 *            从数据库查的表
	 * @param dbView
	 *            放到内存中的表明
	 * @param sql
	 *            从内存中操作表
	 * @param databaseAddress
	 *            服务器databaseAddress地址
	 * @param port
	 *            网络端口
	 * @param account
	 *            数据库名
	 * @param user
	 *            用户名
	 * @param password
	 *            密码
	 * @return 返回数据库内容
	 */
	public Dataset<Row> operateByDataBaseInfo(String dbtable, String dbView, String sql) {
		return oracleDao.daoAdaptByDataBaseInfo(dbtable, dbView, sql, databaseAddress, port, account, user, password,master);
	}
	
	/**
	 * @param 原生数据数据库操作
	 *		 批量插入
	 */
	
	 public int update(String sql) {
	        Connection conn = null;
	        PreparedStatement ps = null;
	        int rs = 0;
	        try {
	            conn = OracleDaoOrigin.getConnection(resultDatabaseAddress, resultAccount, resultUser, resultPassword,outPutPort);
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
