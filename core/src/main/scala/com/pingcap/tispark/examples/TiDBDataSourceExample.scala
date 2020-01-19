package com.pingcap.tispark.examples

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object TiDBDataSourceExample {
  val sparkConf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("TiDBDataSourceExample")
  val spark: SparkSession = SparkSession.builder
    .config(sparkConf)
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    showData()
    saveData()
  }

  //
  def saveData(): Unit = {
    //save data, you can find the data in TIDB database:
    val sc = spark.sparkContext
    import spark.implicits._
    val rdd: RDD[Int] = sc.makeRDD(List(11, 22, 33))

    def saveDataFrame: DataFrame = rdd.toDF("id")
    // TODO: SaveMode: Overwrite is not supported. TiSpark only support SaveMode.Append
    saveDataFrame.write.format("tidb").mode("append")
      .option("spark.tispark.pd.addresses", "127.0.0.1:2379")
      .option("tidb.addr", "127.0.0.1")
      .option("tidb.port", "4000")
      .option("database", "spark01")
      .option("tidb.user", "root")
      .option("tidb.password", "")
      .option("table", "t1")
      //Related Links https://github.com/pingcap/tispark/blob/master/docs/datasource_api_userguide.md#tidb-version-and-configuration-for-write
      // DO NOT set spark.tispark.write.without_lock_table to true on production environment (you may lost data)
      // if tidb <= 3.x, please set the spark.tispark.write.without_lock_table=true
      //TiDB's version must be 4.0 or later.
      // # enable-table-lock is used to control the table lock feature. The default value is false, indicating that the table lock feature is disabled.
      // enable-table-lock: true in config file: tidb.toml
      // why: com.pingcap.tikv.exception.TiBatchWriteException: current tidb does not support LockTable or is disabled!
      .option("spark.tispark.write.without_lock_table", "true")
      .save()
  }

  def showData(): Unit = {
    val dataFrame: DataFrame = spark.read.format("tidb")
      .option("spark.tispark.pd.addresses", "127.0.0.1:2379")
      .option("tidb.addr", "127.0.0.1")
      .option("tidb.port", "4000")
      .option("database", "TPCH_001")
      .option("tidb.user", "root")
      .option("tidb.password", "")
      .option("table", "lineitem")
      .load()

    dataFrame.show()
  }

}
