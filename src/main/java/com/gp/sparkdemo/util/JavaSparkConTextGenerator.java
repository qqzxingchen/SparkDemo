package com.gp.sparkdemo.util;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Spark Demo 的易用静态方法，生成一些本地测试用的 SparkContext
 */
public class JavaSparkConTextGenerator {

    private JavaSparkConTextGenerator(){}

    public static JavaSparkContext generateLocalSparkContext(Integer cores){
        return generateLocalSparkContext( "XCSparkTest",cores );
    }

    public static JavaSparkContext generateLocalSparkContext(){
        return generateLocalSparkContext( "XCSparkTest",null );
    }

    private static JavaSparkContext generateLocalSparkContext( String taskName,Integer coreNumber ){
        SparkConf conf = new SparkConf();

        // Spark 应用名
        conf.set("spark.app.name", taskName);

        if ( coreNumber == null ){
            // Spark 在本地模式启动，且用尽可能多的核心
            conf.set("spark.master", "local[*]");
        } else {
            conf.set("spark.master", String.format("local[%s]", coreNumber.toString()));
        }

        // 使用这个配置对象创建一个SparkContext
        return new JavaSparkContext(conf);
    }

}
