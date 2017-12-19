package com.gp.sparkdemo.core;

import com.gp.sparkdemo.util.LogCache;
import com.gp.sparkdemo.util.JavaSparkConTextGenerator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * aggregateByKey 算子，求一个 rdd 的聚合
 */
public class aggregateByKey {
    public static void main(String[] args){
        LogCache.clearLogs();

        JavaSparkContext sc = JavaSparkConTextGenerator.generateLocalSparkContext();

        JavaPairRDD<Integer,Integer> input = sc.parallelizePairs(new ArrayList<Tuple2<Integer, Integer>>(){{
            for ( int i = 1 ; i < 10 ; i ++ ){
                add(new Tuple2<>(1,i));
                add(new Tuple2<>(2,i+10));
                add(new Tuple2<>(3,i+100));
            }
        }});

        JavaPairRDD<Integer,Tuple2<Integer,Integer>> aggregateResultRDD = input.aggregateByKey(
                new Tuple2<Integer,Integer>(0, 0),
                new Function2<Tuple2<Integer, Integer>, Integer, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> x, Integer y) throws Exception {
                        LogCache.addLog(String.format("seqOp: %s %s", x.toString(), y.toString()));
                        return new Tuple2<>(x._1() + y, x._2() + 1);
                    }
                },
                new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> x, Tuple2<Integer, Integer> y) throws Exception {
                        LogCache.addLog(String.format("comOp: %s %s", x.toString(), y.toString()));
                        return new Tuple2<>(x._1() + y._1(), x._2() + y._2());
                    }
                }
        );

        LogCache.addLog( aggregateResultRDD.collect().toString() );

        LogCache.printLogs();

    }
}
