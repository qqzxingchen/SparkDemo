package com.gp.sparkdemo.core;

import com.gp.sparkdemo.util.JavaSparkConTextGenerator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * join 算子
 * 函数会输出两个 RDD 中 key 相同的所有项，并将它们的 value 联结起来，它联结的 key 要求在两个表中都存在，类似于 SQL 中的 INNER JOIN。
 * 但它不满足交换律，a.join(b) 与 b.join(a) 的结果不完全相同，值插入的顺序与调用关系有关。
 */
public class join {
    public static void main(String[] args){
        JavaSparkContext sc = JavaSparkConTextGenerator.generateLocalSparkContext();

        JavaPairRDD<Integer,Integer> input1 = sc.parallelizePairs(new ArrayList<Tuple2<Integer,Integer>>(){{
            add(new Tuple2<>(1,2));
            add(new Tuple2<>(1,3));
            add(new Tuple2<>(2,2));
            add(new Tuple2<>(3,2));
        }});

        JavaPairRDD<Integer,Integer> input2 = sc.parallelizePairs(new ArrayList<Tuple2<Integer,Integer>>(){{
            add(new Tuple2<>(1,3));
            add(new Tuple2<>(1,3));
            add(new Tuple2<>(2,3));
            add(new Tuple2<>(2,4));
            add(new Tuple2<>(5,3));
            add(new Tuple2<>(6,3));
        }});

        System.out.println( input1.join(input2).collect().toString() );

    }
}
