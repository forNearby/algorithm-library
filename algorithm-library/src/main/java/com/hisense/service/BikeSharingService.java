/**
 * 
 */
package com.hisense.service;

import com.hisense.dao.DataBaseEntity;
import com.hisense.dao.VehicleDaoUtil;

/**
 * @author machao
 * @date 2018年11月12日
 * 共享单车数据相关操作
 */
public class BikeSharingService {
	private VehicleDaoUtil daoUtil = new VehicleDaoUtil();
	public BikeSharingService(DataBaseEntity dataBaseEntity) {
		daoUtil.setDataBase(dataBaseEntity);
	}
}
