package com.gp.sparkdemo.core;

import com.gp.sparkdemo.util.JavaSparkConTextGenerator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * cartesian 算子，求两个 rdd 的笛卡尔积
 */
public class cartesian {
    public static void main(String[] args){
        JavaSparkContext sc = JavaSparkConTextGenerator.generateLocalSparkContext(1);

        JavaPairRDD<Integer,Integer> data1 = sc.parallelizePairs(new ArrayList<Tuple2<Integer, Integer>>(){{
            add(new Tuple2<>(1,2));
            add(new Tuple2<>(1,3));
        }});
        JavaPairRDD<Integer,Integer> data2 = sc.parallelizePairs(new ArrayList<Tuple2<Integer, Integer>>(){{
            add(new Tuple2<>(2,3));
            add(new Tuple2<>(2,4));
            add(new Tuple2<>(1,3));
        }});

        JavaPairRDD< Tuple2<Integer, Integer>,Tuple2<Integer, Integer> > result = data1.cartesian( data2 );

        System.out.println( result.collect().toString() );
    }
}
