package com.yiban.datacenter.Spark_Hive

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkHiveDemo {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Spark-Hive").setMaster("local")
    val sc=new SparkContext(conf)
    
    //create hivecontext
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    
    sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ")
    
    sqlContext.sql("LOAD DATA INPATH '/user/liujiyu/spark/kv1.txt' INTO TABLE src  ");
    
    sqlContext.sql(" SELECT * FROM jn1").collect().foreach(println)
    
    sc.stop()
    
  }
  
  
  
}