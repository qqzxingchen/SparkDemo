package com.gp.sparkdemo.core;

import com.gp.sparkdemo.util.JavaSparkConTextGenerator;
import com.gp.sparkdemo.util.LogCache;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * distinct 算子，去重
 */
public class distinct {
    public static void main(String[] args){
        JavaSparkContext sc = JavaSparkConTextGenerator.generateLocalSparkContext();

        LogCache.clearLogs();

        JavaRDD<Integer> input = sc.parallelize(new ArrayList<Integer>(){{
            add( 1 );
            add( 2 );
            add( 3 );
            add( 2 );
            add( 1 );
            add( 2 );
            add( 3 );
        }});
        LogCache.addLog( input.distinct().collect().toString() );

        JavaPairRDD<Integer,Integer> inputPair = sc.parallelizePairs(new ArrayList<Tuple2<Integer,Integer>>(){{
            add( new Tuple2<>(1,1) );
            add( new Tuple2<>(2,2) );
            add( new Tuple2<>(3,3) );
            add( new Tuple2<>(1,2) );
            add( new Tuple2<>(2,1) );
            add( new Tuple2<>(3,3) );
        }});
        LogCache.addLog( inputPair.distinct().collect().toString() );

        LogCache.printLogs();
    }
}
