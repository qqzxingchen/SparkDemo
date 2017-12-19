package com.gp.sparkdemo.core;

import com.gp.sparkdemo.util.JavaSparkConTextGenerator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * sortByKey 算子
 * 针对键值对 rdd ，根据 key 进行排序
 */
public class sortByKey {
    public static void main(String[] args){
        JavaSparkContext sc = JavaSparkConTextGenerator.generateLocalSparkContext();

        JavaPairRDD<String,Integer> input = sc.parallelizePairs(new ArrayList<Tuple2<String,Integer>>(){{
            add(new Tuple2<>( "a",2 ));
            add(new Tuple2<>( "b",1 ));
            add(new Tuple2<>( "c",3 ));
            add(new Tuple2<>( "a",4 ));
            add(new Tuple2<>( "a",5 ));
            add(new Tuple2<>( "a",6 ));
            add(new Tuple2<>( "d",1 ));
            add(new Tuple2<>( "e",23 ));
            add(new Tuple2<>( "c",4 ));
            add(new Tuple2<>( "a",123 ));
            add(new Tuple2<>( "c",345 ));
            add(new Tuple2<>( "d",12 ));
        }});

        System.out.println( input.sortByKey().collect().toString() );

    }
}
