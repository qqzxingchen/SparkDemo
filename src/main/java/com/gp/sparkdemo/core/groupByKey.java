package com.gp.sparkdemo.core;

import com.gp.sparkdemo.util.JavaSparkConTextGenerator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * groupByKey 算子
 * 针对键值对进行处理，将具有相同 key 的元素汇总起来
 */
public class groupByKey {
    public static void main(String[] args){
        JavaSparkContext sc = JavaSparkConTextGenerator.generateLocalSparkContext();

        JavaPairRDD<String,Integer> inputPairRDD = sc.parallelizePairs(new ArrayList<Tuple2<String,Integer>>(){{
            add( new Tuple2<>( "a",10 ) );
            add( new Tuple2<>( "b",11 ) );
            add( new Tuple2<>( "c",12 ) );
            add( new Tuple2<>( "a",20 ) );
            add( new Tuple2<>( "b",23 ) );
        }});

        System.out.println(inputPairRDD.groupByKey().collect().toString());

    }
}
