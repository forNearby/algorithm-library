package com.hisense.algorithm.bikesharing.util;

import java.math.BigDecimal;
/**
 * @author machao
 * @date 2018年11月12日
 */
public class Line {  
	  
	  
    /** 
     * 端点1 
     */  
    public Point POINTA;  


    /** 
     * 端点2 
     */  
    public Point POINTB;  


    Line(Point pointA, Point pointB) {  


        this.POINTA = pointA;  
        this.POINTB = pointB;  
    }  


    /** 
     * 判断当前线段是否包含给定的点</br> 
     * 即给定的点是否在当前边上 
     * @param point 
     * @return 
     */  
    public boolean isContainsPoint(Point point) {  


        boolean result = false;  
        //判断给定点point与端点1构成线段的斜率是否和当前线段的斜率相同  
        //给定点point与端点1构成线段的斜率k1  
        Double k1 = null;  
        if (point.X.equals(this.POINTA.X)) {  
            k1 = Double.NEGATIVE_INFINITY;  
        } else {  
            k1 = div(sub(point.Y, this.POINTA.Y), sub(point.X, this.POINTA.X));  
        }  
        //当前线段的斜率k2  
        Double k2 = null;  
        if (this.POINTB.X.equals(this.POINTA.X)) {  
            k2 = Double.NEGATIVE_INFINITY;  
        } else {  
            k2 = div(sub(this.POINTB.Y, this.POINTA.Y), sub(this.POINTB.X, this.POINTA.X));  
        }  
        if (k1 != null && k2 != null) {  
            if (k1.equals(k2)) {  
                //若斜率相同，继续判断给定点point的x是否在pointA.x和pointB.x之间,若在 则说明该点在当前边上  
                if (sub(point.X, this.POINTA.X) * sub(point.X, this.POINTB.X) < 0) {  
                    result = true;  
                }  
            }  
        }  
        return result;  
    }  


    //叉积  
    double mult(Point a, Point b, Point c) {  
        return (a.X-c.X)*(b.Y-c.Y)-(b.X-c.X)*(a.Y-c.Y);  
    }  


    /** 
     * 给定线段是否与当前线段相交</br> 
     * 相交返回true, 不相交返回false 
     * @param line 
     * @return 
     */  
    public boolean  
    isIntersect(Line line) {  


        Point aa = this.POINTA;  
        Point bb = this.POINTB;  
        Point cc = line.POINTA;  
        Point dd = line.POINTB;  
        if (Math.max(aa.X, bb.X) < Math.min(cc.X, dd.X)) {  
            return false;  
        }  
        if (Math.max(aa.Y, bb.Y) < Math.min(cc.Y, dd.Y)) {  
            return false;  
        }  
        if (Math.max(cc.X, dd.X) < Math.min(aa.X, bb.X)) {  
            return false;  
        }  
        if (Math.max(cc.Y, dd.Y) < Math.min(aa.Y, bb.Y)) {  
            return false;  
        }  
        if (mult(cc, bb, aa) * mult(bb, dd, aa) < 0 ) {  
            return false;  
        }  
        if ( mult(aa, dd, cc) * mult(dd, bb, cc) < 0 ) {  
            return false;  
        }  
        return true;  
    }  


    /** 
     * 提供精确的加法运算。 
     * @param v1 被加数 
     * @param v2 加数 
     * @return 两个参数的和 
     */  
    public static double add(double v1,double v2){  
        BigDecimal b1 = new BigDecimal(Double.toString(v1));  
        BigDecimal b2 = new BigDecimal(Double.toString(v2));  
        return b1.add(b2).doubleValue();  
    }  


    /** 
     * 提供精确的减法运算。 
     * @param v1 被减数 
     * @param v2 减数 
     * @return 两个参数的差 
     */  
    public static double sub(double v1,double v2){  
        BigDecimal b1 = new BigDecimal(Double.toString(v1));  
        BigDecimal b2 = new BigDecimal(Double.toString(v2));  
        return b1.subtract(b2).doubleValue();  
    }  


    /** 
     * 提供精确的乘法运算。 
     * @param v1 被乘数 
     * @param v2 乘数 
     * @return 两个参数的积 
     */  
    public static double mul(double v1,double v2){  
        BigDecimal b1 = new BigDecimal(Double.toString(v1));  
        BigDecimal b2 = new BigDecimal(Double.toString(v2));  
        return b1.multiply(b2).doubleValue();  
    }  


    private static final int DEF_DIV_SCALE = 10;  


    /** 
     * 提供（相对）精确的除法运算，当发生除不尽的情况时，精确到 
     * 小数点以后10位，以后的数字四舍五入。 
     * @param v1 被除数 
     * @param v2 除数 
     * @return 两个参数的商 
     */  
    public static double div(double v1,double v2){  
        return div(v1,v2,DEF_DIV_SCALE);  
    }  


    /** 
     * 提供（相对）精确的除法运算。当发生除不尽的情况时，由scale参数指 
     * 定精度，以后的数字四舍五入。 
     * @param v1 被除数 
     * @param v2 除数 
     * @param scale 表示表示需要精确到小数点以后几位。 
     * @return 两个参数的商 
     */  
    public static double div(double v1,double v2,int scale){  
        if(scale<0){  
            throw new IllegalArgumentException(  
                    "The scale must be a positive integer or zero");  
        }  
        BigDecimal b1 = new BigDecimal(Double.toString(v1));  
        BigDecimal b2 = new BigDecimal(Double.toString(v2));  
        return b1.divide(b2,scale,BigDecimal.ROUND_HALF_UP).doubleValue();  
    }  
}  
