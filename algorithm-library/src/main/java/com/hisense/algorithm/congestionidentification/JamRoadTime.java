package com.hisense.algorithm.congestionidentification;


import java.util.Date;

/**
 * @Author: maxiao
 * @Date: 2018/11/5 16:47
 */
public class JamRoadTime {
    private String fbd;
    private Date startTime;
    private Date endTime;

    public String getFbd() {
        return fbd;
    }

    public void setFbd(String fbd) {
        this.fbd = fbd;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }
}
