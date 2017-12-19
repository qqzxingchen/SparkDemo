package com.gp.sparkdemo.core;

import com.gp.sparkdemo.util.JavaSparkConTextGenerator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * cogroup 算子
 * 将多个 RDD 中同一个 key 对应的 value 组合到一起
 */
public class cogroup {
    public static void main(String[] args){
        JavaSparkContext sc = JavaSparkConTextGenerator.generateLocalSparkContext();

        JavaPairRDD<Integer,Integer> data1 = sc.parallelizePairs(new ArrayList<Tuple2<Integer, Integer>>(){{
            add(new Tuple2<>(1,2));
            add(new Tuple2<>(1,3));
            add(new Tuple2<>(2,2));
            add(new Tuple2<>(3,2));
        }});

        JavaPairRDD<Integer,Integer> data2 = sc.parallelizePairs(new ArrayList<Tuple2<Integer, Integer>>(){{
            add(new Tuple2<>(2,3));
            add(new Tuple2<>(2,4));
            add(new Tuple2<>(1,3));
            add(new Tuple2<>(5,3));
            add(new Tuple2<>(6,3));
        }});

        System.out.println( data1.cogroup(data2).collect() );

    }
}
