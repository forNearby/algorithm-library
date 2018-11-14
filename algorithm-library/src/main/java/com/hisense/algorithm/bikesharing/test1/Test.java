/**
 * 
 */
package com.hisense.algorithm.bikesharing.test1;

import java.util.ArrayList;
import java.util.List;

import com.hisense.algorithm.bikesharing.util.MapUtil;
import com.hisense.algorithm.bikesharing.util.Point;

/**
 * @author machao
 * @date 2018年11月12日
 * 获取点是不是在多边形内
 */
public class Test {
	public static void main(String[] args) {  
        List<Point> points = new ArrayList<>(); //多变形区域
        Point point11 = new Point(104.074688, 30.544471);  
        Point point22 = new Point(104.077167, 30.544549);  
        Point point33 = new Point(104.077042, 30.541718);  
        Point point44 = new Point(104.074814, 30.541578);  
        Point point55 = new Point(104.074742, 30.544393);  
        points.add(point11);  
        points.add(point22);  
        points.add(point33);  
        points.add(point44);  
        points.add(point55);  
        Point test = new Point(104.07564, 30.542817);  
        if(MapUtil.isPointInPolygon(test, points))
        {
            System.out.println("点在区域内");  
        }else
        {
        	System.out.println("点不在区域内");
        }
    }  
}
