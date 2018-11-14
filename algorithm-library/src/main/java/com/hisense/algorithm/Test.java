package com.hisense.algorithm;

import com.alibaba.fastjson.JSONObject;
import com.hisense.algorithm.frequenttrafficjam.TrafficRoads;
import com.hisense.dao.DataBaseEntity;

import java.io.IOException;

/**
 * @author machao
 * @date 2018年10月31日
 */
public class Test {

    public static void main(String[] args) throws IOException {
//        String jSONString = IoUtil.readFile("D:\\IDEAProjects\\ChengDu\\src\\com\\hisense\\util\\data.json", "UTF-8");
        String jSONString = "{\"datasource\":{\"car_info\":{\"account\":\"hiatmp\",\"database_address\":\"10.16.3.22\",\"password\":\"hiamp2020\",\"tablename\":\"car_info\"},\"road_info\":{\"account\":\"hiatmp\",\"database_address\":\"10.16.3.22\",\"password\":\"hiamp2020\",\"tablename\":\"road_info\"},\"td_road_status\":{\"account\":\"hiatmp\",\"database_address\":\"192.168.0.108\",\"user\":\"jhgx\",\"password\":\"jhgx\",\"tablename\":\"td_road_status\"},\"TD_VEHICLE_RECORD_DAY\":{\"account\":\"hiatmp\",\"database_address\":\"192.168.0.108\",\"user\":\"jhgx\",\"password\":\"jhgx\",\"tablename\":\"TD_VEHICLE_RECORD_DAY\"},\"TS_DEVICE\":{\"account\":\"hiatmp\",\"database_address\":\"192.168.0.108\",\"user\":\"jhgx\",\"password\":\"jhgx\",\"tablename\":\"TS_DEVICE\"}},\"operational_param\":{\"model\":\"CongestionIdentification\",\"roadid\":\"u3333\"},\"output\":{\"result_info\":{\"account\":\"hiatmp\",\"database_address\":\"192.168.0.108\",\"user\":\"jhgx\",\"password\":\"jhgx\",\"tablename\":\"TD_JAMROAD_TEST\"},\"TD_VEHICLE_ONROAD\":{\"account\":\"hiatmp\",\"database_address\":\"192.168.0.108\",\"user\":\"jhgx\",\"password\":\"jhgx\",\"tablename\":\"TD_VEHICLE_ONROAD\"}},\"spark_info\":{\"master\":\"local\"}}";
//        String jSONString = args[0];
        JSONObject jsonObject = JSONObject.parseObject(jSONString);
        JSONObject datasource = jsonObject.getJSONObject("datasource");
        JSONObject tdRoadStatus = datasource.getJSONObject("td_road_status");
        String account = tdRoadStatus.getString("account");
        String databaseAddress = tdRoadStatus.getString("database_address");
        String user = tdRoadStatus.getString("user");
        String password = tdRoadStatus.getString("password");
        String tableName = tdRoadStatus.getString("tableName");
        String master = jsonObject.getJSONObject("spark_info").getString("master");

        JSONObject output = jsonObject.getJSONObject("output");
        JSONObject result_info = output.getJSONObject("result_info");
        String result_account = result_info.getString("account");
        String result_database_address = result_info.getString("database_address");
        String result_user = result_info.getString("user");
        String result_password = result_info.getString("password");
        String result_tablename = result_info.getString("tablename");


        DataBaseEntity dBEntity = new DataBaseEntity();
        dBEntity.setAccount(account);
        dBEntity.setDatabaseAddress(databaseAddress);
        dBEntity.setUser(user);
        dBEntity.setPassword(password);
        dBEntity.setTableName(tableName);
        dBEntity.setMaster(master);

        dBEntity.setOutPutAccount(result_account);
        dBEntity.setOutPutDatabaseAddress(result_database_address);
        dBEntity.setOutPutUser(result_user);
        dBEntity.setOutPutPassword(result_password);
        dBEntity.setOutPutTablename(result_tablename);

        String model = jsonObject.getJSONObject("operational_param").getString("model");
        if ("CongestionIdentification".equals(model)) {
            TrafficRoads test = new TrafficRoads(dBEntity);
            long startTime = System.currentTimeMillis();
            test.workdayService();
            test.weekdayService();
            test.workdayMorningPeakService();
            test.workdayEveningPeakService();
            test.whichDayTrafficJam();
            long endTime = System.currentTimeMillis();
            System.out.println("程序运行时间： " + (endTime - startTime) + "ms");
        }
    }
}
