package com.hisense.algorithm.bikesharing.util;

import java.util.List;
/**
 * @author machao
 * @date 2018年11月12日
 * 多边形区域坐标检测
 */
public class MapUtil {    	  
    /** 
     * 给定点和多边形，判断给定的点是否在多边O形内 
     * @param point 
     * @param points 
     * @return 
     */  
    public static boolean isPointInPolygon(Point point, List<Point> points) {  


        boolean result = false;  
        int intersectCount = 0;  
        // 判断依据：求解从该点向右发出的水平线射线与多边形各边的交点，当交点数为奇数，则在内部  
        //不过要注意几种特殊情况：1、点在边或者顶点上;2、点在边的延长线上;3、点出发的水平射线与多边形相交在顶点上  
        /** 
         * 具体步骤如下： 
         * 循环遍历各个线段： 
         *  1、判断点是否在当前边上(斜率相同,且该点的x值在两个端口的x值之间),若是则返回true 
         *  2、否则判断由该点发出的水平射线是否与当前边相交,若不相交则continue 
         *  3、若相交,则判断是否相交在顶点上(边的端点是否在给定点的水平右侧).若不在,则认为此次相交为穿越,交点数+1 并continue 
         *  4、若交在顶点上,则判断上一条边的另外一个端点与当前边的另外一个端点是否分布在水平射线的两侧.若是则认为此次相交为穿越,交点数+1. 
         */  
        for (int i = 0; i < points.size(); i++) {  
            Point pointA = points.get(i);  
            Point pointB = null;  
            Point pointPre = null;  
            //若当前是第一个点,则上一点则是list里面的最后一个点  
            if (i == 0) {  
                pointPre = points.get(points.size() - 1);  
            } else {  
                pointPre = points.get(i - 1);  
            }  
            //若已经循环到最后一个点,则与之连接的是第一个点  
            if (i == (points.size() - 1)) {  
                pointB = points.get(0);  
            } else {  
                pointB = points.get(i + 1);  
            }  
            Line line = new Line(pointA, pointB);  
            //1、判断点是否在当前边上(斜率相同,且该点的x值在两个端口的x值之间),若是则返回true  
            boolean isAtLine = line.isContainsPoint(point);  
            if (isAtLine) {  
                return true;  
            } else {  
                //2、若不在边上,判断由该点发出的水平射线是否与当前边相交,若不相交则continue  
                //设置该射线的另外一个端点的x值=999,保证边的x永远不超过  
                Point  radialPoint = new Point(999d, point.Y);  
                Line radial = new Line(point, radialPoint);  
                boolean isIntersect = radial.isIntersect(line);  
                if (!isIntersect) {  
                    continue;  
                } else {  
                    //3、若相交,则判断是否相交在顶点上(边的端点是否在给定点的水平右侧).若不在,则认为此次相交为穿越,交点数+1 并continue  
                    if (!( (pointA.X > point.X) && (pointA.Y.equals(point.Y))  
                            || (pointB.X > point.X) && (pointB.Y.equals(point.Y)) )) {  
                        intersectCount++;  
                        continue;  
                    } else {  
                        //4、若交在顶点上,则判断上一条边的另外一个端点与当前边的另外一个端点是否分布在水平射线的两侧.若是则认为此次相交为穿越,交点数+1  
                        if ((pointPre.Y - point.Y) * (pointB.Y - point.Y) < 0) {  
                            intersectCount++;  
                        }  
                    }  
                }  
            }  
        }  
        result = intersectCount % 2 == 1;  
        return result;  
    }  
}  
