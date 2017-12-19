package com.gp.sparkdemo.core;

import com.gp.sparkdemo.util.JavaSparkConTextGenerator;
import com.gp.sparkdemo.util.TestDataGenerateTool;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * flatMap 算子
 * 类似于 map 算子，只是它可以一次输出多条记录
 */
public class flatMap {
    public static void main(String[] args){
        JavaSparkContext sc = JavaSparkConTextGenerator.generateLocalSparkContext();

        JavaRDD<Integer> inputRDD = sc.parallelize(TestDataGenerateTool.range(10,50));

        JavaRDD<String> flatMapedRDD = inputRDD.flatMap(new FlatMapFunction<Integer,String>(){
            @Override
            public Iterator<String> call(Integer v){
                return new ArrayList<String>(){{
                    add( Integer.toString(v) );
                    add( Integer.toString(v+1000) );
                }}.iterator();
            }
        });

        System.out.println( flatMapedRDD.collect().toString() );
    }
}
