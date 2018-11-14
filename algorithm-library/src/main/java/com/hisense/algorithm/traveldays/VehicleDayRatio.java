/**
 * 
 */
package com.hisense.algorithm.traveldays;

/**
 * @author machao
 * @date 2018年11月6日
 * 不同出行天数下的车辆数量及占比
 */
public class VehicleDayRatio {
private String vehicleDay;
private long vehicleNumber;
private String vehicleDayRatio;
public String getVehicleDay() {
	return vehicleDay;
}
public void setVehicleDay(String vehicleDay) {
	this.vehicleDay = vehicleDay;
}
public long getVehicleNumber() {
	return vehicleNumber;
}
public void setVehicleNumber(long vehicleNumber) {
	this.vehicleNumber = vehicleNumber;
}
public String getVehicleDayRatio() {
	return vehicleDayRatio;
}
public void setVehicleDayRatio(String vehicleDayRatio) {
	this.vehicleDayRatio = vehicleDayRatio;
}
@Override
public String toString() {
	return "VehicleDayRatio [vehicleDay=" + vehicleDay + ", vehicleNumber=" + vehicleNumber + ", vehicleDayRatio="
			+ vehicleDayRatio + "]";
}




}
