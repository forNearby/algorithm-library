/**
 * 
 */
package com.hisense.algorithm.commuter.maxmin;


/**
 * @author machao
 * @date 2018年10月30日
 * 车辆数据点
 */
public class Point {
	private String entityID;
	private double peakFrequency;
	private double unpeakFrequency;
	private double onRoadTime;
	private int peakFrequencyPosition;
	private int unpeakFrequencyPosition;
	private int onRoadTimePosition;	

	public String getEntityID() {
		return entityID;
	}

	public void setEntityID(String entityID) {
		this.entityID = entityID;
	}

	public Double getPeakFrequency() {
		return peakFrequency;
	}

	public void setPeakFrequency(double peakFrequency) {
		this.peakFrequency = peakFrequency;
	}

	public Double getUnpeakFrequency() {
		return unpeakFrequency;
	}

	public void setUnpeakFrequency(double unpeakFrequency) {
		this.unpeakFrequency = unpeakFrequency;
	}

	public Double getOnRoadTime() {
		return onRoadTime;
	}

	public void setOneRoadTime(double onRoadTime) {
		this.onRoadTime = onRoadTime;
	}



	public int getPeakFrequencyPosition() {
		return peakFrequencyPosition;
	}

	public void setPeakFrequencyPosition(int peakFrequencyPosition) {
		this.peakFrequencyPosition = peakFrequencyPosition;
	}

	public int getUnpeakFrequencyPosition() {
		return unpeakFrequencyPosition;
	}

	public void setUnpeakFrequencyPosition(int unpeakFrequencyPosition) {
		this.unpeakFrequencyPosition = unpeakFrequencyPosition;
	}

	public int getOnRoadTimePosition() {
		return onRoadTimePosition;
	}

	public void setOnRoadTimePosition(int onRoadTimePosition) {
		this.onRoadTimePosition = onRoadTimePosition;
	}

	public void setOnRoadTime(double onRoadTime) {
		this.onRoadTime = onRoadTime;
	}

	@Override
	public String toString() {
		return "车辆 [entityID=" + entityID + ", peakFrequency=" + peakFrequency + ", unpeakFrequency="
				+ unpeakFrequency + ", oRoadTime=" + onRoadTime + "]";
	}

	public double distance(Point entity) {
		double product = Math.pow(peakFrequency - entity.getPeakFrequency(), 2);
		product += Math.pow(unpeakFrequency - entity.getUnpeakFrequency(), 2);
		product += Math.pow(onRoadTime - entity.getOnRoadTime(), 2);
		return Math.abs(Math.sqrt(product));
	}
}
