package com.gp.sparkdemo.core;

import com.gp.sparkdemo.util.JavaSparkConTextGenerator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;

/**
 * intersection 算子
 * 只返回两个 RDD 中都有的元素，同时保证返回的 RDD 中不包含重复的元素
 */
public class intersection {
    public static void main(String[] args){
        JavaSparkContext sc = JavaSparkConTextGenerator.generateLocalSparkContext();

        JavaRDD<Integer> input1 = sc.parallelize(new ArrayList<Integer>(){{
            add(1);
            add(2);
            add(3);
            add(4);
            add(5);
        }});

        JavaRDD<Integer> input2 = sc.parallelize(new ArrayList<Integer>(){{
            add(2);
            add(3);
            add(2);
            add(3);
            add(5);
        }});

        System.out.println( input1.intersection(input2).collect().toString() );

    }
}
