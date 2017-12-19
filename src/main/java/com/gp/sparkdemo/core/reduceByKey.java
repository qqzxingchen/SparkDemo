package com.gp.sparkdemo.core;

import com.gp.sparkdemo.util.JavaSparkConTextGenerator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * reduceByKey 算子
 * 针对键值对进行处理，将具有相同 key 的元素汇总起来，并通过 func 函数进行汇总计算
 */
public class reduceByKey {
    public static void main(String[] args){
        JavaSparkContext sc = JavaSparkConTextGenerator.generateLocalSparkContext();

        JavaPairRDD<String,Integer> input = sc.parallelizePairs(new ArrayList<Tuple2<String,Integer>>(){{
            add(new Tuple2<>("a",10));
            add(new Tuple2<>("b",11));
            add(new Tuple2<>("a",12));
        }});

        JavaPairRDD<String,Integer> result = input.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2+10;
            }
        });

        System.out.println( result.collect().toString() );

    }
}
