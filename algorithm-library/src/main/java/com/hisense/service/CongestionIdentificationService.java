package com.hisense.service;

import com.hisense.algorithm.congestionidentification.JamRoadTime;
import com.hisense.dao.DaoUtil;
import com.hisense.dao.DataBaseEntity;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 * @Author: maxiao
 * @Date: 2018/11/2 18:35
 */
public class CongestionIdentificationService {
    private static Logger logger = Logger.getLogger(CongestionIdentificationService.class);
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


    public CongestionIdentificationService(DataBaseEntity dataBaseEntity) {
        account = dataBaseEntity.getAccount();
        databaseAddress = dataBaseEntity.getDatabaseAddress();
        user = dataBaseEntity.getUser();
        password = dataBaseEntity.getPassword();
        tableName = dataBaseEntity.getTableName();
        master = dataBaseEntity.getMaster();
        port = dataBaseEntity.getPort();

        outPutAccount = dataBaseEntity.getOutPutAccount();
        outPutDatabaseAddress = dataBaseEntity.getOutPutDatabaseAddress();
        outPutUser = dataBaseEntity.getOutPutUser();
        outPutPassword = dataBaseEntity.getOutPutPassword();
        outPutTablename = dataBaseEntity.getOutPutTablename();
        outPutPort = dataBaseEntity.getOutPutPort();
    }

    public List<Row> getJamRoad(int n) {
        DaoUtil daoUtil = new DaoUtil();
        String sql = "(select FBD, TIME " +
                " from " + tableName +
                " where STATUS = 3" +
                "  AND TIME < trunc(sysdate) - 1" +
                "  AND TIME > trunc(sysdate) - 31" +
                " order by FBD,TIME ) dbtable";
        Dataset<Row> workdayDS = daoUtil.operateByDataBaseInfo(sql, "mark", "SELECT * FROM mark", databaseAddress, port, account, user, password, master);
        return workdayDS.collectAsList();
    }

    public List<Row> getManyDayJamRoad(int n) {
        DaoUtil daoUtil = new DaoUtil();
        String sql = "(SELECT *" +
                " FROM (SELECT FBD, to_date(to_char(TIME, 'hh24:mi:ss'), 'hh24:mi:ss') AS times, count(TIME) AS amount" +
                "       FROM " + tableName +
                "       WHERE STATUS = 3" +
                "         AND TIME < trunc(sysdate) - 1" +
                "         AND TIME > trunc(sysdate) - 31" +
                "       GROUP BY FBD, to_char(TIME, 'hh24:mi:ss')" +
                "       ORDER BY FBD, to_char(TIME, 'hh24:mi:ss'))" +
                " WHERE amount > 18) dbtable";
        Dataset<Row> manyDayJamRoad = daoUtil.operateByDataBaseInfo(sql, "mark", "SELECT * FROM mark", databaseAddress, port, account, user, password, master);
        return manyDayJamRoad.collectAsList();
    }

    /**
     * @param sql 更新数据库
     * @return 插入数据库返回插入结果
     */
    public int update(String sql) {
        DaoUtil daoUtil = new DaoUtil();
        int result = daoUtil.update(sql, outPutDatabaseAddress, outPutAccount, outPutUser, outPutPassword, outPutPort);
        return result;
    }

    /**
     * @param roads 拥堵路口时间
     * @param model 1为单日拥堵时段，2为多日拥堵时段
     */
    public void insertDB(List<JamRoadTime> roads, int model) {
        final int batchSize = 1000;
        StringBuilder sb = new StringBuilder();
        if (roads.size() < 1) {
            return;
        }
        int count = 30;
        sb.append("insert all ");
        if (model == 1) {
            for (int i = 0; i < roads.size(); i++) {
                JamRoadTime road = roads.get(i);
                String fbd = road.getFbd();
                String startTime;
                String endTime;
                DateFormat fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                startTime = fm.format(road.getStartTime());
                endTime = fm.format(road.getEndTime());
                if (count > 0) {
                    System.out.println("拥堵路口为" + fbd + "拥堵开始时间为" + startTime + "拥堵结束将时间为" + endTime);
                    logger.info("拥堵路口为" + fbd + "拥堵开始时间为" + startTime + "拥堵结束将时间为" + endTime);
                    count--;
                }
                sb.append(" into " + outPutTablename + " values");
                sb.append("(");
                sb.append("sys_guid(),");
                sb.append("'" + fbd + "',");
                sb.append("'" + startTime + "',");
                sb.append("'" + endTime + "'");
                sb.append(")");
                int temp = i % batchSize;
                if (temp == 0 && i > 999) {
                    sb.append("select 1 from dual");
                    update(sb.toString());
                    sb.delete(10, sb.length());
                }
            }
        } else {
            for (int i = 0; i < roads.size(); i++) {
                JamRoadTime road = roads.get(i);
                String fbd = road.getFbd();
                String startTime;
                String endTime;
                DateFormat fm = new SimpleDateFormat("HH:mm:ss");
                startTime = fm.format(road.getStartTime());
                endTime = fm.format(road.getEndTime());
                if (count > 0) {
                    System.out.println("拥堵路口为" + fbd + "拥堵开始时间为" + startTime + "拥堵结束将时间为" + endTime);
                    logger.info("拥堵路口为" + fbd + "拥堵开始时间为" + startTime + "拥堵结束将时间为" + endTime);
                    count--;
                }
                sb.append(" into " + outPutTablename + " values");
                sb.append("(");
                sb.append("sys_guid(),");
                sb.append("'" + fbd + "',");
                sb.append("'" + startTime + "',");
                sb.append("'" + endTime + "'");
                sb.append(")");
                int temp = i % batchSize;
                if (temp == 0 && i > 999) {
                    sb.append("select 1 from dual");
                    update(sb.toString());
                    sb.delete(10, sb.length());
                }
            }
        }
        if (sb.length() > 12) {
            sb.append("select 1 from dual");
            update(sb.toString());
        }
    }
}
