package com.hisense.algorithm.congestionidentification;

import com.hisense.dao.DataBaseEntity;
import com.hisense.service.CongestionIdentificationService;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @Author: maxiao
 * @Date: 2018/11/2 18:34
 */
public class TrafficTime {
    CongestionIdentificationService service;
    private String outPutTablename;
    private static Logger logger = Logger.getLogger(TrafficTime.class);

    /**
     * @param dataBaseEntity 根据配置文件获取数据库连接
     */
    public TrafficTime(DataBaseEntity dataBaseEntity) {
        service = new CongestionIdentificationService(dataBaseEntity);
        outPutTablename = dataBaseEntity.getOutPutTablename();
    }

    /**
     * @param n 需要统计的天数
     * @return 返回单日拥堵时段
     */
    public List<JamRoadTime> jungleTrafficTime(int n) {
        List<Row> roadName = service.getJamRoad(n);
        if (roadName.size() == 0) {
            return new ArrayList<>();
        }
        int count = 0;
        boolean notContinuous;
        String fbdtemp = roadName.get(0).getString(0);
        Date startTime = new Date(roadName.get(0).getTimestamp(1).getTime());
        Date endTime = new Date(roadName.get(0).getTimestamp(1).getTime());
        Date afterDate = new Date(startTime.getTime() + 120000);
        List<JamRoadTime> jamRoadTimes = new ArrayList<>();

        for (Row aroadName1 : roadName) {

            String fbd = aroadName1.getString(0);
            Date time = new Date(aroadName1.getTimestamp(1).getTime());


            if (fbdtemp.equals(fbd)) {
                if (time.equals(afterDate)) {
                    count++;
                    endTime = time;
                    notContinuous = false;
                } else {
                    notContinuous = true;
                    if (count > 4) {
                        JamRoadTime jamRoadTime = new JamRoadTime();
                        jamRoadTime.setFbd(fbdtemp);
                        jamRoadTime.setStartTime(startTime);
                        jamRoadTime.setEndTime(endTime);
                        jamRoadTimes.add(jamRoadTime);

                    }
                }
                if (notContinuous) {
                    startTime = time;
                    count = 0;
                }
                afterDate = new Date(time.getTime() + 120000);
            } else {
                fbdtemp = fbd;
                startTime = time;
                endTime = time;
                afterDate = new Date(time.getTime() + 120000);
            }
        }
        if (startTime != endTime && count > 2) {
            JamRoadTime jamRoadTime = new JamRoadTime();
            jamRoadTime.setFbd(fbdtemp);
            jamRoadTime.setStartTime(startTime);
            jamRoadTime.setEndTime(endTime);
        }
        return combine(jamRoadTimes, 10);
    }

    /**
     * @param n 连续拥堵的天数
     * @return 返回多日拥堵时段
     */
    public List<JamRoadTime> manyDay(int n) {
        int daynum = (int) (n * 0.6);
        List<Row> roadName = service.getManyDayJamRoad(daynum);
        if (roadName.size() == 0) {
            return new ArrayList<>();
        }
        List<JamRoadTime> jamRoadTimesManyDay = new ArrayList<>();
        int count = 0;
        boolean notContinuous;
        String fbdtemp = roadName.get(0).getString(0);
        Date startTime = new Date(roadName.get(0).getTimestamp(1).getTime());
        Date endTime = new Date(roadName.get(0).getTimestamp(1).getTime());
        Date afterDate = new Date(startTime.getTime() + 120000);


        for (Row aroadName1 : roadName) {
            String fbd = aroadName1.getString(0);
            Date time = new Date(aroadName1.getTimestamp(1).getTime());

            if (fbdtemp.equals(fbd)) {
                if (time.equals(afterDate)) {
                    count++;
                    endTime = time;
                    notContinuous = false;
                } else {
                    notContinuous = true;
                    if (count > 4) {
                        JamRoadTime jamRoadTime = new JamRoadTime();
                        jamRoadTime.setFbd(fbdtemp);
                        jamRoadTime.setStartTime(startTime);
                        jamRoadTime.setEndTime(endTime);
                        jamRoadTimesManyDay.add(jamRoadTime);
                    }
                }
                if (notContinuous) {
                    startTime = time;
                    count = 0;
                }
                afterDate = new Date(time.getTime() + 120000);
            } else {
                fbdtemp = fbd;
                startTime = time;
                endTime = time;
                afterDate = new Date(time.getTime() + 120000);
            }
        }
        return combine(jamRoadTimesManyDay, 15);
    }

    /**
     * @param jamRoad  拥堵的路口信息
     * @param interval 时间间隔，在多少时间间隔内会对两条记录进行合并
     * @return 返回合并后的拥堵路口信息
     */
    public List<JamRoadTime> combine(List<JamRoadTime> jamRoad, int interval) {
        List<JamRoadTime> newjamRoad = new ArrayList<>();
        if (jamRoad.size() == 0) {
            return newjamRoad;
        } else {
            JamRoadTime temp = jamRoad.get(0);
            for (int i = 1; i < jamRoad.size(); i++) {
                JamRoadTime jamRoadTime = jamRoad.get(i);
                if (temp.getFbd().equals(jamRoadTime.getFbd())) {
                    if (((jamRoadTime.getEndTime().getTime() - temp.getEndTime().getTime()) / 60000) <= interval) {
                        temp.setEndTime(jamRoadTime.getEndTime());
                    } else {
                        newjamRoad.add(temp);
                        temp = jamRoadTime;
                    }
                } else {
                    newjamRoad.add(temp);
                    temp = jamRoadTime;
                }
            }
            newjamRoad.add(temp);
            return newjamRoad;
        }
    }

    /**
     * 插入数据库
     *
     * @param mark  拥堵路口时间
     * @param model 1为单日拥堵时段，2为多日拥堵时段
     */
    public void insertDB(List<JamRoadTime> mark, int model) {
        service.insertDB(mark, model);
    }
}

