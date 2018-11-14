package com.hisense.service;

import com.hisense.dao.DaoUtil;
import com.hisense.dao.DataBaseEntity;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.math.BigDecimal;
import java.util.*;

/**
 * @Author: maxiao
 * @Date: 2018/10/31 15:17
 */
public class FrequentTrafficJamService {
    private static Logger logger = Logger.getLogger(FrequentTrafficJamService.class);
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


    public FrequentTrafficJamService(DataBaseEntity dataBaseEntity) {
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

    /**
     * 获取道路名称
     *
     * @return 返回数据库内容
     */
    public String[] getRoadName() {
        DaoUtil daoUtil = new DaoUtil();
        String dbtable = "(select distinct (FBD) from " + tableName + ") dbtable";
        Dataset<Row> dataset = daoUtil.operateBySql(dbtable, "mark", "SELECT * FROM mark");
        List<Row> list = dataset.collectAsList();
        String[] roadName = new String[list.size()];
        for (int i = 0; i < list.size(); i++) {
            roadName[i] = list.get(i).toString().replace("[", "").replace("]", "");
        }
        return roadName;
    }

    /**
     * 获取工作日状态
     *
     * @return 返回数据库内容
     */
    public int getWorkday() {
        DaoUtil daoUtil = new DaoUtil();
        String dbtable = "(SELECT FBD, COUNT(STATUS), STATUS , to_char(TIME, 'yyyymmdd')" +
                " FROM " + tableName + " WHERE (to_char(TIME, 'd') != 1 AND to_char(TIME, 'd') != 7 AND STATUS = 3 " +
                "  AND TIME < trunc(sysdate) - 1" +
                "  AND TIME > sysdate - 31" +
                ") GROUP BY FBD, STATUS, to_char(TIME, 'yyyymmdd') ORDER BY FBD) dbtable";
        Dataset<Row> workdayDS = daoUtil.operateByDataBaseInfo(dbtable, "mark", "SELECT * FROM mark", databaseAddress, port, account, user, password, master);
        List<Row> temp = workdayDS.collectAsList();
        int result = universalMethod(temp, 60, 7, "为工作日常发堵点", "0");
        return result;
    }

    /**
     * 获取非工作日客流数据
     *
     * @return 返回数据库内容
     */
    public int getWeekday() {
        DaoUtil daoUtil = new DaoUtil();
        String dbtable = "(SELECT FBD, COUNT(STATUS), STATUS , to_char(TIME, 'yyyymmdd') " +
                " FROM " + tableName + " WHERE (to_char(TIME, 'd') = 1 AND to_char(TIME, 'd') = 7 AND STATUS = 3 " +
                "  AND TIME < trunc(sysdate) - 1" +
                "  AND TIME > sysdate - 31" +
                ") GROUP BY FBD, STATUS, to_char(TIME, 'yyyymmdd') ORDER BY FBD) dbtable";
        Dataset<Row> weekdayDS = daoUtil.operateByDataBaseInfo(dbtable, "mark", "SELECT * FROM mark", databaseAddress, port, account, user, password, master);
        List<Row> temp = weekdayDS.collectAsList();
        int result = universalMethod(temp, 30, 21, "为非工作日常发堵点", "1");
        return result;
    }

    /**
     * 获取工作日早高峰客流数据
     *
     * @return 返回数据库内容
     */
    public int getWorkdayMorningPeak() {
        DaoUtil daoUtil = new DaoUtil();
        String dbtable = "(SELECT FBD, COUNT(STATUS), STATUS , to_char(TIME, 'yyyymmdd') " +
                " FROM " + tableName + " WHERE (to_char(TIME, 'd') != 1 AND to_char(TIME, 'd') != 7 AND STATUS = 3 " +
                "  AND TIME < trunc(sysdate) - 1" +
                "  AND TIME > sysdate - 31" +
                " AND to_char(TIME, 'hh24') > 5 AND to_char(TIME, 'hh24') < 12 ) " +
                " GROUP BY FBD, STATUS, to_char(TIME, 'yyyymmdd') ORDER BY FBD) dbtable";
        Dataset<Row> workdayMorningPeakDS = daoUtil.operateByDataBaseInfo(dbtable, "mark", "SELECT * FROM mark", databaseAddress, port, account, user, password, master);
        List<Row> temp = workdayMorningPeakDS.collectAsList();
        int result = universalMethod(temp, 30, 7, "为工作日早高峰常发堵点", "2");
        return result;
    }

    /**
     * 获取工作日晚高峰客流数据
     *
     * @return 返回数据库内容
     */
    public int getWorkdayEveningPeak() {
        DaoUtil daoUtil = new DaoUtil();
        String dbtable = "(SELECT FBD, COUNT(STATUS), STATUS , to_char(TIME, 'yyyymmdd') " +
                " FROM " + tableName + " WHERE (to_char(TIME, 'd') != 1 AND to_char(TIME, 'd') != 7 AND STATUS = 3 " +
                "  AND TIME < trunc(sysdate) - 1" +
                "  AND TIME > sysdate - 31" +
                " AND to_char(TIME, 'hh24') > 14 AND to_char(TIME, 'hh24') < 23 ) " +
                " GROUP BY FBD, STATUS, to_char(TIME, 'yyyymmdd') ORDER BY FBD) dbtable";
        Dataset<Row> workdayEveningPeakDS = daoUtil.operateByDataBaseInfo(dbtable, "mark", "SELECT * FROM mark", databaseAddress, port, account, user, password, master);
        List<Row> temp = workdayEveningPeakDS.collectAsList();
        int result = universalMethod(temp, 30, 7, "为工作日晚高峰常发堵点", "3");
        return result;
    }

    /**
     * 获取周n全天客流数据
     *
     * @param n 周几
     * @return 返回数据库内容
     */
    public int getDayWorkdayData(int n) {
        String[] workday = {"4", "5", "6", "7", "8", "9", "10"};
        int day = n + 1;
        DaoUtil daoUtil = new DaoUtil();
        String dbtable = "(SELECT FBD, COUNT(STATUS), STATUS , to_char(TIME, 'yyyymmdd') FROM " + tableName +
                " WHERE (to_char(TIME, 'd') = " + String.valueOf(day) +
                "  AND TIME < trunc(sysdate) - 1" +
                "  AND TIME > sysdate - 31" +
                "  AND STATUS = 3 ) GROUP BY FBD, STATUS, to_char(TIME, 'yyyymmdd') " +
                " ORDER BY FBD) dbtable";
        Dataset<Row> dayWorkdayDataDS = daoUtil.operateByDataBaseInfo(dbtable, "mark", "SELECT * FROM mark", databaseAddress, port, account, user, password, master);
        List<Row> temp = dayWorkdayDataDS.collectAsList();
        int result = universalMethod(temp, 60, 2, "在周" + String.valueOf(n) + "为日常发堵点", workday[n - 1]);
        return result;
    }

    /**
     * 获取周n早晚高峰客流数据
     *
     * @param n 周几
     * @return 返回数据库内容
     */
    public int getPeakWorkdayData(int n) {
        String[] workdaypeak = {"11", "12", "13", "14", "15"};
        int day = n + 1;
        DaoUtil daoUtil = new DaoUtil();
        String dbtable = "(SELECT FBD, COUNT(STATUS), STATUS , to_char(TIME, 'yyyymmdd') FROM " + tableName +
                " WHERE (to_char(TIME, 'd') = " + String.valueOf(day) +
                "  AND TIME < trunc(sysdate) - 1" +
                "  AND TIME > sysdate - 31" +
                "  AND STATUS = 3  AND ((to_char(TIME, 'hh24') > 14 AND to_char(TIME, 'hh24') < 23) OR " +
                " (to_char(TIME, 'hh24') > 5 AND to_char(TIME, 'hh24') < 12))) GROUP BY FBD, STATUS, " +
                " to_char(TIME, 'yyyymmdd') ORDER BY FBD) dbtable";
        Dataset<Row> peakWorkdayDataDS = daoUtil.operateByDataBaseInfo(dbtable, "mark", "SELECT * FROM mark", databaseAddress, port, account, user, password, master);
        List<Row> temp = peakWorkdayDataDS.collectAsList();
        int result = universalMethod(temp, 30, 2, "在周" + String.valueOf(n) + "为早晚高峰发堵点", workdaypeak[n - 1]);
        return result;
    }


    /**
     * @param sql 更新数据库
     * @return 返回是否成功
     */
    public int update(String sql) {
        DaoUtil daoUtil = new DaoUtil();
        int result = daoUtil.update(sql, outPutDatabaseAddress, outPutAccount, outPutUser, outPutPassword, outPutPort);
        return result;
    }


    /**
     * @param day         从数据库中获取的数据
     * @param minuteLimit 超过多少时间算拥堵
     * @param dayLimit    不超过多少天不进行计算
     * @param reminder    如果出现拥堵，进行的提示
     * @param type        具体插入那个字段
     */
    public int universalMethod(List<Row> day, int minuteLimit, int dayLimit, String reminder, String type) {
        Set<Object> set = new HashSet<>();
        Map<String, Integer> map = new HashMap<>();
        for (Row aDay : day) {
            String fbd = aDay.getString(0);
            int count = ((BigDecimal) aDay.get(1)).intValue();
            set.add(aDay.get(3));
            if (count > minuteLimit) {
                if (!map.containsKey(fbd)) {
                    map.put(fbd, 1);
                } else {
                    int i = map.get(fbd);
                    i += 1;
                    map.put(fbd, i);
                }
            }
        }
        int setSize = set.size();
        int threshold = (int) (setSize * 0.6);
        StringBuilder sb = new StringBuilder();
        int result = 0;
        int i = 0;
        final int batchSize = 1000;
        sb.append("insert all ");
        for (String key : map.keySet()) {
            if (map.get(key) > threshold && map.get(key) > dayLimit) {
//            if (map.get(key) > 1) {
                i++;
                System.out.println("路口" + key + reminder);
                logger.info("路口" + key + reminder);
                sb.append(" into " + outPutTablename + " values");
                sb.append("(");
                sb.append("sys_guid(),");
                sb.append("'" + key + "',");
                sb.append("'" + type + "',");
                sb.append("sysdate");
                sb.append(")");
                int temp = i % batchSize;
                if (temp == 0 && i > 999) {
                    sb.append(" select 1 from dual");
                    result = update(sb.toString());
                    sb.delete(10, sb.length());
                }
            }
        }
        if (sb.length() > 12) {
            sb.append("select 1 from dual");
            update(sb.toString());
        }
        return result;
    }
}
