package com.gp.sparkdemo.core;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.gp.sparkdemo.util.JavaSparkConTextGenerator;
import com.gp.sparkdemo.util.TestDataGenerateTool;
import org.apache.spark.api.java.function.Function;


/**
 * map 算子
 * 根据传给 map 的函数对原 rdd 中的每个元素进行映射，得到新的 rdd
 */
public class map {
    public static void main(String[] args){

        JavaSparkContext sc = JavaSparkConTextGenerator.generateLocalSparkContext();

        JavaRDD<Integer> inputRangeList = sc.parallelize(TestDataGenerateTool.range(10,100));

        JavaRDD<String> stringRDD = inputRangeList.map(new Function<Integer,String>(){
            @Override
            public String call(Integer x){
                return String.format("data: %s",x.toString());
            }
        });

        System.out.println( stringRDD.collect().toString() );
    }
}
