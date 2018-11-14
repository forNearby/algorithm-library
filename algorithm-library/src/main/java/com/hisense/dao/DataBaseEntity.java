/**
 *
 */
package com.hisense.dao;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONObject;
import com.hisense.algorithm.frequenttrafficjam.JtjTest;

/**
 * @author machao
 * @date 2018年11月3日
 */
public class DataBaseEntity {
	private static Logger logger = Logger.getLogger(DataBaseEntity.class);
    private String account;
    private String databaseAddress;
    private String user;
    private String password;
    private String tableName;
    private String master;
    private String port;
    private String outPutAccount;
    private String outPutDatabaseAddress;
    private String outPutUser;
    private String outPutPassword;
    private String outPutTablename;
    private String outPutPort;

    public String getOutPutPort() {
        return outPutPort;
    }

    public void setOutPutPort(String outPutPort) {
        this.outPutPort = outPutPort;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getDatabaseAddress() {
        return databaseAddress;
    }

    public void setDatabaseAddress(String databaseAddress) {
        this.databaseAddress = databaseAddress;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public String getOutPutAccount() {
        return outPutAccount;
    }

    public void setOutPutAccount(String outPutAccount) {
        this.outPutAccount = outPutAccount;
    }

    public String getOutPutDatabaseAddress() {
        return outPutDatabaseAddress;
    }

    public void setOutPutDatabaseAddress(String outPutDatabaseAddress) {
        this.outPutDatabaseAddress = outPutDatabaseAddress;
    }

    public String getOutPutUser() {
        return outPutUser;
    }

    public void setOutPutUser(String outPutUser) {
        this.outPutUser = outPutUser;
    }

    public String getOutPutPassword() {
        return outPutPassword;
    }

    public void setOutPutPassword(String outPutPassword) {
        this.outPutPassword = outPutPassword;
    }

    public String getOutPutTablename() {
        return outPutTablename;
    }

    public void setOutPutTablename(String outPutTablename) {
        this.outPutTablename = outPutTablename;
    }


    @Override
    public String toString() {
        return "DataBaseEntity [account=" + account + ", databaseAddress=" + databaseAddress + ", user=" + user
                + ", password=" + password + ", tableName=" + tableName + ", master=" + master + ", port=" + port
                + ", outPutAccount=" + outPutAccount + ", outPutDatabaseAddress=" + outPutDatabaseAddress
                + ", outPutUser=" + outPutUser + ", outPutPassword=" + outPutPassword + ", outPutTablename="
                + outPutTablename + "]";
    }

    public void getInfo(String jSONString, String dataSourceTable, String outPutTable) {
    	try{
        JSONObject jsonObject = JSONObject.parseObject(jSONString);
        JSONObject datasource = jsonObject.getJSONObject("datasource");
        if (jsonObject.containsKey("spark_info")) {
            JSONObject sparkInfo = jsonObject.getJSONObject("spark_info");
            this.master = sparkInfo.getString("master");
        } else {
            this.master = null;
        }

        if (dataSourceTable != null && !"".equals(dataSourceTable)) {
            JSONObject tdRoadStatus = datasource.getJSONObject(dataSourceTable);
            String accountRemote = tdRoadStatus.getString("database");
            String databaseAddressRemote = tdRoadStatus.getString("database_address");
            String userRemote = tdRoadStatus.getString("account");
            String passwordRemote = tdRoadStatus.getString("password");
            String tableNameRemote = tdRoadStatus.getString("tablename");
            if (tdRoadStatus.containsKey("port")) {
                this.port = tdRoadStatus.getString("port");
            } else {
                this.port = "1521";
            }
            this.account = accountRemote;
            this.databaseAddress = databaseAddressRemote;
            this.user = userRemote;
            this.password = passwordRemote;
            this.tableName = tableNameRemote;

        }
        if (outPutTable != null && !"".equals(outPutTable)) {

            JSONObject outPutJsonOri = jsonObject.getJSONObject("output");
            JSONObject outPutJson = outPutJsonOri.getJSONObject(outPutTable);
            String outPutAccount = outPutJson.getString("database");
            String outPutDatabaseAddress = outPutJson.getString("database_address");
            String outPutUser = outPutJson.getString("account");
            String outPutPassword = outPutJson.getString("password");
            String outPutTablename = outPutJson.getString("tablename");
            if (outPutJson.containsKey("port")) {
                this.outPutPort = outPutJson.getString("port");
            } else {
                this.outPutPort = "1521";
            }
            this.outPutAccount = outPutAccount;
            this.outPutDatabaseAddress = outPutDatabaseAddress;
            this.outPutUser = outPutUser;
            this.outPutPassword = outPutPassword;
            this.outPutTablename = outPutTablename;
        }
    	}catch(Exception e){
    		logger.error(e.getMessage());
    	}
    }
}
