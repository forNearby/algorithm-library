package com.hisense.algorithm.commuter.maxmin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import org.apache.log4j.Logger;

/**
 * @author machao
 * @date 2018年10月30日 最大最小距离聚类算法 寻找聚类中心和聚类集 判断通勤车所在聚类集合
 */
public class MaxMinDistance {
	private static Logger logger = Logger.getLogger(MaxMinDistance.class);
	private static final double t = 0.5;
	private static final double MAX_VALUE = Double.MAX_VALUE;
	private   double sumT=0.0;
	private int entitiesSzie;
	private List<Point> entities;

	public MaxMinDistance(List<Point> entities) {
		this.entities = entities;
		this.entitiesSzie=entities.size();
	}

	private Point center;

	/**
	 * 聚类中心集，选取距离所有样本中心(均值)最近的一个样本作为第一个初始聚类中心
	 */
	public MaxMinDistance makeCenter() {
		logger.info("--------------------------------选取第一个初始聚类中心---------------------------------");
		TreeMap<Double, Point> queue = new TreeMap<Double, Point>();
		for (Point src : entities) {
			sumT=0.0;
//			double sum = 0.0;
//			for (Point e : entities) {
//				if (src.getEntityID().equals(e.getEntityID()))
//					continue;
//				sum += src.distance(e);
//			}
			getThread(src,entities.subList(0, entitiesSzie/4)).start();
			getThread(src,entities.subList(entitiesSzie/4,entitiesSzie/2)).start();
			getThread(src,entities.subList(entitiesSzie/2,(entitiesSzie/4)*3)).start();
			getThread(src,entities.subList((entitiesSzie/4)*3, entitiesSzie)).start();
			queue.put(sumT / (double) entities.size(), src);
		}
		center = queue.entrySet().iterator().next().getValue();
		entities.remove(center);
		logger.info("---------------------------------首选聚类中心："+center+"------------------------------");
		logger.info(center);

		return this;
	}
	/**
	 *第一个初始聚类中心多线程任务分配
	 */
	 private  Thread getThread(Point src,List<Point> entitiesT) {

	        return new Thread(){
	               @Override
	               public void run() {
	            	   for (Point e : entitiesT) {
	           			if (src.getEntityID().equals(e.getEntityID()))
	           			{
	           				continue;
	           			}else
	           			{
	           				sumT += src.distance(e);

	           			}
	           		}  
	               }
	        };
	 }
	private Point furthestCenter;

	/**
	 * 取一个距离中心距离最大的点，作为第二个聚类中心,并计算阈值T
	 */
	public MaxMinDistance makeFurthestCenter() {
		logger.info("---------------开始-----------------选取第二个初始聚类中心-------------------------------");
		TreeMap<Double, Point> queue = new TreeMap<Double, Point>();
		for (Point e : entities) {
			queue.put(MAX_VALUE - e.distance(center), e);
		}
		furthestCenter = queue.entrySet().iterator().next().getValue();
		entities.remove(furthestCenter);

		logger.info("---------------完成----------中心最远距离：" + furthestCenter+"-------------------------");

		return this;
	}

	private List<Point> otherCenters;

	/**
	 * 寻找所有的聚类中心
	 */
	public MaxMinDistance makeOtherCenter() {
		logger.info("----------------开始-------------寻找所有的聚类中心---------------------------------");
		double furthest = furthestCenter.distance(center);
		ArrayList<Point> meansKs = new ArrayList<Point>();
		meansKs.add(center);
		meansKs.add(furthestCenter);

		TreeMap<Double, Point> queue = new TreeMap<Double, Point>();
		do {
			queue.clear();
			for (Point e : entities) {
				double distence = MAX_VALUE;
				for (Point c : meansKs)
					distence = Math.min(distence, e.distance(c));
				queue.put(MAX_VALUE - distence, e);
			}
		} while (entities.size() > 0 && MAX_VALUE - queue.entrySet().iterator().next().getKey() > furthest * t
				&& meansKs.add(queue.entrySet().iterator().next().getValue())
				&& entities.remove(queue.entrySet().iterator().next().getValue()));

		meansKs.remove(center);
		meansKs.remove(furthestCenter);
		otherCenters = meansKs;

		logger.info("-------------------完成----------------计算其他聚类中心距离--------------------------");
		// otherCenters.forEach((e) -> logger.info(e));

		return this;
	}

	/**
	 * 对所有样本点进行聚类，存入polys
	 */
	private List<List<Point>> polys;

	public MaxMinDistance poly() {
		logger.info("---------------开始-------------------对所有样本点进行聚类--------------------------");
		LinkedList<Point> meansKs = new LinkedList<Point>(otherCenters);
		meansKs.addFirst(furthestCenter);
		meansKs.addFirst(center);

		HashMap<String, List<Point>> kmeans = new HashMap<String, List<Point>>();
		TreeMap<Double, Point> queue = new TreeMap<Double, Point>();
		for (Point e : entities) {
			queue.clear();
			for (Point m : meansKs) {
				queue.put(e.distance(m), m);
			}
			Point k = queue.entrySet().iterator().next().getValue();
			if (!kmeans.containsKey(k.getEntityID())) {
				kmeans.put(k.getEntityID(), new ArrayList<>());
				kmeans.get(k.getEntityID()).add(k);
			}

			kmeans.get(k.getEntityID()).add(e);
		}
		polys = new ArrayList<>(kmeans.values());
		// for(List<Point> points:polys)
		// {
		// for(Point point:points)
		// {
		// System.out.print(point );
		// }
		// logger.info();
		//
		// }

		return this;
	}

	/**
	 * 分别对将聚类中心的第n个特征变量,进行排序
	 */
	public MaxMinDistance sort() {
		logger.info("------------------------分别对将聚类中心的第n个特征变量,进行排序-------------------------");
		int num = 0;
		ArrayList<Point> src = new ArrayList<Point>();
		src.addAll(entities);
		src.addAll(otherCenters);
		src.add(center);
		src.add(furthestCenter);

		src.sort((a, b) -> b.getPeakFrequency().compareTo(a.getPeakFrequency()));
		// 第一个特征变量（工作日早晚高峰车辆使用频度）按照降序进行排列
		num = 1;
		for (int i = 0; i < src.size(); i++)
			if (i == 0
					|| src.get(i - 1).getPeakFrequency().doubleValue() == src.get(i).getPeakFrequency().doubleValue())
				src.get(i).setPeakFrequencyPosition(num);
			else
				src.get(i).setPeakFrequencyPosition(++num);

		src.sort((a, b) -> a.getUnpeakFrequency().compareTo(b.getUnpeakFrequency()));
		// 第二个特征变量（工作日平峰车辆使用频度；）按照升序进行排列
		num = 1;
		for (int i = 0; i < src.size(); i++)
			if (i == 0
					|| src.get(i - 1).getUnpeakFrequency().doubleValue() == src.get(i).getUnpeakFrequency()
							.doubleValue())
				src.get(i).setUnpeakFrequencyPosition(num);
			else
				src.get(i).setUnpeakFrequencyPosition(++num);

		src.sort((a, b) -> b.getOnRoadTime().compareTo(a.getOnRoadTime()));
		// 第三个特征变量（工作日车辆早晚高峰时段在第1次检测断面位置的出现概率）按照降序进行排列
		num = 1;
		for (int i = 0; i < src.size(); i++)
			if (i == 0 || src.get(i - 1).getOnRoadTime().doubleValue() == src.get(i).getOnRoadTime().doubleValue())
				src.get(i).setOnRoadTimePosition(num);
			else
				src.get(i).setOnRoadTimePosition(++num);
		return this;
	}

	/**
	 * 对数据进行筛选,得到通勤车所在的类
	 */
	public List<Point> sift() {
		logger.info("-----------------------------------对数据进行筛选,得到通勤车所在的类------------------------------------");
		TreeMap<Integer, List<List<Point>>> queue = new TreeMap<Integer, List<List<Point>>>();
		for (List<Point> e : polys) {
			int sum = 0;
			for (Point k : e)
				sum += k.getPeakFrequencyPosition() + k.getUnpeakFrequencyPosition() + k.getOnRoadTimePosition();
			if (!queue.containsKey(sum))
				queue.put(sum, new ArrayList<>());
			queue.get(sum).add(e);
		}
		List<List<Point>> commuterCars = queue.entrySet().iterator().next().getValue();
		if (commuterCars.size() > 1) {
			queue.clear();
			for (List<Point> e : commuterCars) {
				int sum = 0;
				for (Point k : e)
					sum += k.getPeakFrequencyPosition();
				if (!queue.containsKey(sum))
					queue.put(sum, new ArrayList<>());
				queue.get(sum).add(e);
			}
			commuterCars = queue.entrySet().iterator().next().getValue();
		}
		// logger.info("二次筛选通勤车：");
		// commuterCars.forEach((e) -> e.forEach((ex) ->
		// logger.info(e)));
		// } else {
		// logger.info("筛选通勤车：");
		// commuterCars.get(0).forEach((e) -> logger.info(e));
		// logger.info("通勤车数量 ：" + commuterCars.get(0).size());
		// }
		return commuterCars.get(0);
	}
}
