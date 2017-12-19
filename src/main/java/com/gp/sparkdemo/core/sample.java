package com.gp.sparkdemo.core;

import com.gp.sparkdemo.util.JavaSparkConTextGenerator;
import com.gp.sparkdemo.util.LogCache;
import com.gp.sparkdemo.util.TestDataGenerateTool;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * sample 算子
 * 随机抽样
 * sample( withReplacement, fraction, seed=None)
 *      withReplacement
 *          True 表示有放回的抽样 (即即使原 rdd 不存在重复数据，抽样结果也可能存在重复数据)
 *          False 表示无放回的抽样 (即如果原 rdd 不存在重复数据，则抽样结果肯定不存在重复数据)
 *      fraction  一个浮点数，该数表示抽样的概率。比如 fraction = 0.5 ，则表示 rdd 中每个元素都有 50% 的概率被抽取出来
 *      seed      一个整形，表示抽样的随机数的种子
 *          即对于同一 rdd 以及相同的 seed ，则抽样结果一定一样
 */
public class sample {
    public static void main(String[] args){
        JavaSparkContext sc = JavaSparkConTextGenerator.generateLocalSparkContext();

        JavaRDD<Integer> input = sc.parallelize(TestDataGenerateTool.range(0,100));

        input.persist(StorageLevel.MEMORY_ONLY());

        LogCache.clearLogs();
        for ( int i = 0 ; i < 10 ; i ++ ){
            LogCache.addLog( input.sample( false,0.1,i ).collect().toString() );
        }

        LogCache.printLogs();
    }
}
