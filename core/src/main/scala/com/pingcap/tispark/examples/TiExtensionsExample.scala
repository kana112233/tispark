package com.pingcap.tispark.examples

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TiExtensionsExample {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", getClass.getName)
      .setIfMissing("spark.sql.extensions", "org.apache.spark.sql.TiExtensions")
      .setIfMissing("tidb.addr", "127.0.0.1")
      .setIfMissing("tidb.port", "4000")
      .setIfMissing("tidb.user", "root")
      .setIfMissing("spark.tispark.pd.addresses", "127.0.0.1:2379")
      .setIfMissing("spark.tispark.plan.allow_index_read", "true")

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()

    spark.sql("use TPCH_001")
    spark.sql("select count(*) from lineitem").show
  }

}