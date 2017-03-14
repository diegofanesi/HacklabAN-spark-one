package org.hacklaban

import java.sql.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
  * Created by diego on 3/5/17.
  */
object DataLoader {
  val datasetPath: String = "/home/diego/HacklabAN-spark-one/spark-warehouse/"
  var userPay: DataFrame = null
  var shopInfo: DataFrame = null
  var userView: DataFrame = null
  var resultDataset: DataFrame = null

  //Logger.getLogger("org").setLevel(Level.OFF)

  val spark: SparkSession = SparkSession
    .builder().master("spark://192.168.1.145:7077")
    .appName("Spark basic example")
    .config("spark.executor.memory", "10g")
    .getOrCreate()

  import spark.implicits._

  def loadFile(filename: String): DataFrame = return spark.read.format("parquet").option("compression", "gzip").load(datasetPath.+(filename))

  def loadData(): Unit = {
    //Main Datasets
    userPay = (loadFile("userPay.gz.parquet"))
    shopInfo = (loadFile("shopInfo.gz.parquet"))
    userView = (loadFile("userView.gz.parquet"))
  }

  def preprocess(): Unit = {

    userPay = userPay.select(
      col("shop_id"),
      col("user_id"),
      month(col("timestamp")).name("month"),
      dayofmonth(col("timestamp")).name("day"),
      year(col("timestamp")).name("year"),
      date_format(col("timestamp"), "EEEE").name("DoW"),
      date_format(col("timestamp"), "yyyy-MM-dd").name("date")
    ).groupBy(
      col("shop_id"),
      col("year"),
      col("month"),
      col("day"),
      col("DoW"),
      col("date")
    ).count().withColumnRenamed("count", "nevents").coalesce(16).persist(StorageLevel.MEMORY_ONLY)

    userView = userView.select(
      col("shop_id"),
      col("user_id"),
      month(col("timestamp")).name("month"),
      dayofmonth(col("timestamp")).name("day"),
      year(col("timestamp")).name("year"),
      date_format(col("timestamp"), "EEEE").name("DoW"),
      date_format(col("timestamp"), "yyyy-MM-dd").name("date")
    ).groupBy(
      col("shop_id"),
      col("year"),
      col("month"),
      col("day"),
      col("DoW"),
      col("date")
    ).count().withColumnRenamed("count", "nevents").coalesce(16).persist(StorageLevel.MEMORY_ONLY)

    theRightWay()

  }

  def theWrongWay(): Unit = {
    var validShops = userPay.groupBy("shop_id").agg(min(col("date")), max(col("date"))).withColumnRenamed("max(date)", "max").withColumnRenamed("min(date)", "min").filter("datediff(max,min) > 120").persist(StorageLevel.MEMORY_ONLY)

    resultDataset = userPay.join(validShops, userPay.col("shop_id") === validShops.col("shop_id"), "right").filter("datediff(date, min) > 90").select(
      userPay.col("shop_id"),
      col("year"),
      col("month"),
      col("day"),
      col("DoW"),
      col("date"),
      col("nevents"),
      getDataFromHistory(userPay)(date_sub($"date", 1), userPay.col("shop_id")),
      getDataFromHistory(userPay)(date_sub($"date", 2), userPay.col("shop_id")),
      getDataFromHistory(userPay)(date_sub($"date", 3), userPay.col("shop_id")),
      getDataFromHistory(userPay)(date_sub($"date", 4), userPay.col("shop_id")),
      getDataFromHistory(userPay)(date_sub($"date", 5), userPay.col("shop_id")),
      getDataFromHistory(userPay)(date_sub($"date", 6), userPay.col("shop_id")),
      getDataFromHistory(userPay)(date_sub($"date", 7), userPay.col("shop_id")),
      getDataFromHistory(userPay)(date_sub($"date", 8), userPay.col("shop_id")),
      getDataFromHistory(userPay)(date_sub($"date", 9), userPay.col("shop_id")),
      getDataFromHistory(userPay)(date_sub($"date", 10), userPay.col("shop_id")),
      getDataFromHistory(userPay)(date_sub($"date", 11), userPay.col("shop_id")),
      getDataFromHistory(userPay)(date_sub($"date", 12), userPay.col("shop_id")),
      getDataFromHistory(userPay)(date_sub($"date", 13), userPay.col("shop_id")),
      getDataFromHistory(userPay)(date_sub($"date", 14), userPay.col("shop_id")),
      getDataFromHistory(userView)(date_sub($"date", 1), userPay.col("shop_id")),
      getDataFromHistory(userView)(date_sub($"date", 2), userPay.col("shop_id")),
      getDataFromHistory(userView)(date_sub($"date", 3), userPay.col("shop_id")),
      getDataFromHistory(userView)(date_sub($"date", 4), userPay.col("shop_id")),
      getDataFromHistory(userView)(date_sub($"date", 5), userPay.col("shop_id")),
      getDataFromHistory(userView)(date_sub($"date", 6), userPay.col("shop_id")),
      getDataFromHistory(userView)(date_sub($"date", 7), userPay.col("shop_id")),
      getDataFromHistory(userView)(date_sub($"date", 8), userPay.col("shop_id")),
      getDataFromHistory(userView)(date_sub($"date", 9), userPay.col("shop_id")),
      getDataFromHistory(userView)(date_sub($"date", 10), userPay.col("shop_id")),
      getDataFromHistory(userView)(date_sub($"date", 11), userPay.col("shop_id")),
      getDataFromHistory(userView)(date_sub($"date", 12), userPay.col("shop_id")),
      getDataFromHistory(userView)(date_sub($"date", 13), userPay.col("shop_id")),
      getDataFromHistory(userView)(date_sub($"date", 14), userPay.col("shop_id"))

    )
  }

  def theRightWay(): Unit = {
    var validShops = userPay.groupBy("shop_id").agg(min(col("date")), max(col("date"))).withColumnRenamed("max(date)", "max").withColumnRenamed("min(date)", "min").filter("datediff(max,min) > 15").persist(StorageLevel.MEMORY_ONLY)

    resultDataset = userPay.as("uP0").join(validShops.as("validShops"), $"uP0.shop_id" === $"validShops.shop_id", "right").filter("datediff(date, min) > 15")
      .join(userPay.as("uP1"), $"uP0.shop_id" === $"uP1.shop_id" && $"uP1.date" === date_sub($"uP0.date", 1))
      .join(userPay.as("uP2"), $"uP0.shop_id" === $"uP2.shop_id" && $"uP2.date" === date_sub($"uP0.date", 2))
      .join(userPay.as("uP3"), $"uP0.shop_id" === $"uP3.shop_id" && $"uP3.date" === date_sub($"uP0.date", 3))
      .join(userPay.as("uP4"), $"uP0.shop_id" === $"uP4.shop_id" && $"uP4.date" === date_sub($"uP0.date", 4))
      .join(userPay.as("uP5"), $"uP0.shop_id" === $"uP5.shop_id" && $"uP5.date" === date_sub($"uP0.date", 5))
      .join(userPay.as("uP6"), $"uP0.shop_id" === $"uP6.shop_id" && $"uP6.date" === date_sub($"uP0.date", 6))
      .join(userPay.as("uP7"), $"uP0.shop_id" === $"uP7.shop_id" && $"uP7.date" === date_sub($"uP0.date", 7))
      .join(userPay.as("uP8"), $"uP0.shop_id" === $"uP8.shop_id" && $"uP8.date" === date_sub($"uP0.date", 8))
      .join(userPay.as("uP9"), $"uP0.shop_id" === $"uP9.shop_id" && $"uP9.date" === date_sub($"uP0.date", 9))
      .join(userPay.as("uP10"), $"uP0.shop_id" === $"uP10.shop_id" && $"uP10.date" === date_sub($"uP0.date", 10))
      .join(userPay.as("uP11"), $"uP0.shop_id" === $"uP11.shop_id" && $"uP11.date" === date_sub($"uP0.date", 11))
      .join(userPay.as("uP12"), $"uP0.shop_id" === $"uP12.shop_id" && $"uP12.date" === date_sub($"uP0.date", 12))
      .join(userPay.as("uP13"), $"uP0.shop_id" === $"uP13.shop_id" && $"uP13.date" === date_sub($"uP0.date", 13))
      .join(userPay.as("uP14"), $"uP0.shop_id" === $"uP14.shop_id" && $"uP14.date" === date_sub($"uP0.date", 14))
      .join(userView.as("uV1"), $"uP0.shop_id" === $"uV1.shop_id" && $"uV1.date" === date_sub($"uP0.date", 1))
      .join(userView.as("uV2"), $"uP0.shop_id" === $"uV2.shop_id" && $"uV2.date" === date_sub($"uP0.date", 2))
      .join(userView.as("uV3"), $"uP0.shop_id" === $"uV3.shop_id" && $"uV3.date" === date_sub($"uP0.date", 3))
      .join(userView.as("uV4"), $"uP0.shop_id" === $"uV4.shop_id" && $"uV4.date" === date_sub($"uP0.date", 4))
      .join(userView.as("uV5"), $"uP0.shop_id" === $"uV5.shop_id" && $"uV5.date" === date_sub($"uP0.date", 5))
      .join(userView.as("uV6"), $"uP0.shop_id" === $"uV6.shop_id" && $"uV6.date" === date_sub($"uP0.date", 6))
      .join(userView.as("uV7"), $"uP0.shop_id" === $"uV7.shop_id" && $"uV7.date" === date_sub($"uP0.date", 7))
      .join(userView.as("uV8"), $"uP0.shop_id" === $"uV8.shop_id" && $"uV8.date" === date_sub($"uP0.date", 8))
      .join(userView.as("uV9"), $"uP0.shop_id" === $"uV9.shop_id" && $"uV9.date" === date_sub($"uP0.date", 9))
      .join(userView.as("uV10"), $"uP0.shop_id" === $"uV10.shop_id" && $"uV10.date" === date_sub($"uP0.date", 10))
      .join(userView.as("uV11"), $"uP0.shop_id" === $"uV11.shop_id" && $"uV11.date" === date_sub($"uP0.date", 11))
      .join(userView.as("uV12"), $"uP0.shop_id" === $"uV12.shop_id" && $"uV12.date" === date_sub($"uP0.date", 12))
      .join(userView.as("uV13"), $"uP0.shop_id" === $"uV13.shop_id" && $"uV13.date" === date_sub($"uP0.date", 13))
      .join(userView.as("uV14"), $"uP0.shop_id" === $"uV14.shop_id" && $"uV14.date" === date_sub($"uP0.date", 14))
      .join(shopInfo, $"uP0.shop_id" === shopInfo.col("shop_id"))
      .select(
        $"uP0.shop_id",
        $"uP0.year",
        $"uP0.month",
        $"uP0.day",
        $"uP0.DoW",
        $"uP0.date",
        $"city_name",
        $"location_id",
        $"per_pay",
        $"score",
        $"comment_cnt",
        $"shop_level",
        $"category1",
        $"category2",
        $"category3",
        $"uP0.nevents".as("pay_events"),
        $"uP1.nevents".as("pay_events_1"),
        $"uP2.nevents".as("pay_events_2"),
        $"uP3.nevents".as("pay_events_3"),
        $"uP4.nevents".as("pay_events_4"),
        $"uP5.nevents".as("pay_events_5"),
        $"uP7.nevents".as("pay_events_6"),
        $"uP8.nevents".as("pay_events_7"),
        $"uP9.nevents".as("pay_events_8"),
        $"uP6.nevents".as("pay_events_9"),
        $"uP10.nevents".as("pay_events_10"),
        $"uP11.nevents".as("pay_events_11"),
        $"uP12.nevents".as("pay_events_12"),
        $"uP13.nevents".as("pay_events_13"),
        $"uP14.nevents".as("pay_events_14"),
        $"uV1.nevents".as("view_events_1"),
        $"uV2.nevents".as("view_events_2"),
        $"uV3.nevents".as("view_events_3"),
        $"uV4.nevents".as("view_events_4"),
        $"uV5.nevents".as("view_events_5"),
        $"uV7.nevents".as("view_events_6"),
        $"uV8.nevents".as("view_events_7"),
        $"uV9.nevents".as("view_events_8"),
        $"uV6.nevents".as("view_events_9"),
        $"uV10.nevents".as("view_events_10"),
        $"uV11.nevents".as("view_events_11"),
        $"uV12.nevents".as("view_events_12"),
        $"uV13.nevents".as("view_events_13"),
        $"uV14.nevents".as("view_events_14")
      )
  }

  def getDataFromHistory(dataframe: DataFrame) = udf(
    (date: Date, shop_id: Int) => if (dataframe.filter($"shop_id" === shop_id && $"date" == date).count() == 0) 0 else dataframe.filter($"shop_id" === shop_id && $"date" == date).select($"nevents").first().getInt(0)
  )

  def saveDataset(df: DataFrame, filename: String, format: String): Unit = {
    if (format == "csv")
      df.coalesce(1).write.format("csv").option("header", "true").save(datasetPath.+(filename))
    if (format == "parquet")
      df.coalesce(1).write.format("parquet").save(datasetPath.+(filename))
  }

  def main(args: Array[String]): Unit = {
    loadData()
    preprocess()
    resultDataset.show(10)
    saveDataset(resultDataset, "resultDataset", "parquet")
  }
}
