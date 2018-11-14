package com.hisense.algorithm.frequenttrafficjam;

import com.hisense.dao.DataBaseEntity;
import org.apache.log4j.Logger;

/**
 * @Author: maxiao
 * @Date: 2018/11/7 15:46
 */
public class JtjTest {
    private static Logger logger = Logger.getLogger(JtjTest.class);

    public static void main(String[] args) {
        //String jSONString = args[0];
//        String jSONString = new String(java.util.Base64.getDecoder().decode(args[0]));
        String jSONString = new String(java.util.Base64.getDecoder().decode("eyJvdXRwdXQiOnsicmVzdWx0X2luZm8iOnsiZGF0YWJhc2VfYWRkcmVzcyI6IjIwLjIuMTUuMTgiLCJwYXNzd29yZCI6IkpIR1gwMDEiLCJkYXRhYmFzZSI6Imd4ZGIiLCJ0YWJsZW5hbWUiOiJBUl9KQU1ST0FEIiwiYWNjb3VudCI6IkpIR1gifX0sIm9wZXJhdGlvbmFsX3BhcmFtIjp7fSwiZGF0YXNvdXJjZSI6eyJyb2FkX2luZm8iOnsiZGF0YWJhc2VfYWRkcmVzcyI6IjIwLjIuMTUuMTgiLCJwYXNzd29yZCI6IkpIR1gwMDEiLCJkYXRhYmFzZSI6Imd4ZGIiLCJ0YWJsZW5hbWUiOiJ0ZF9yb2FkX3N0YXR1cyIsImFjY291bnQiOiJKSEdYIn19LCJpbnRlcl9yZXN1bHQiOnt9fQ=="));
        logger.info(jSONString);
        System.out.println(jSONString);
        DataBaseEntity dBEntity = new DataBaseEntity();
        logger.error(dBEntity.toString());
        dBEntity.getInfo(jSONString, "road_info", "result_info");
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
