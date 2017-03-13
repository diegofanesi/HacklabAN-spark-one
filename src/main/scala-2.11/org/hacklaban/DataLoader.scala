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
  val datasetPath: String = "/home/diego/dataset/"
  var userPay: DataFrame = null
  var shopInfo: DataFrame = null
  var userView: DataFrame = null

  //Logger.getLogger("org").setLevel(Level.OFF)

  val spark: SparkSession = SparkSession
    .builder().master("local[8]") //.master("spark://192.168.1.145:7077")
    .appName("Spark SQL basic example")
    .config("spark.executor.memory", "5g")
    //.config("spark.executor.cores","16")
    .getOrCreate()

  import spark.implicits._

  def loadFile(filename: String): DataFrame = return spark.read.format("parquet").option("compression", "gzip").load(datasetPath.+(filename))


  def loadData(): Unit = {
    //Main Datasets
    userPay = (loadFile("user_pay.gz.parquet"))
    shopInfo = (loadFile("shop_info.gz.parquet"))
    userView = (loadFile("user_view.gz.parquet"))

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

  }

  def theWrongWay(): Unit = {
    var validShops = userPay.groupBy("shop_id").agg(min(col("date")), max(col("date"))).withColumnRenamed("max(date)", "max").withColumnRenamed("min(date)", "min").filter("datediff(max,min) > 120").persist(StorageLevel.MEMORY_ONLY)

    userPay = userPay.join(validShops, userPay.col("shop_id") === validShops.col("shop_id"), "right").filter("datediff(date, min) > 90").select(
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
    var validShops = userPay.groupBy("shop_id").agg(min(col("date")), max(col("date"))).withColumnRenamed("max(date)", "max").withColumnRenamed("min(date)", "min").filter("datediff(max,min) > 120").persist(StorageLevel.MEMORY_ONLY)

    userPay = userPay.join(validShops, userPay.col("shop_id") === validShops.col("shop_id"), "right").filter("datediff(date, min) > 90").select(
      userPay.col("shop_id"),
      col("year"),
      col("month"),
      col("day"),
      col("DoW"),
      col("date"),
      col("nevents")
    ).as("uP0")
      .join(userPay.as("uP1"), $"uP0.shop_id" === $"uP1.shop_id" && $"uP1.date" === date_sub($"uP0.date", 1))
      .join(userPay.as("uP2"), $"uP0.shop_id" === $"uP2.shop_id" && $"uP2.date" === date_sub($"uP0.date", 1))
      .join(userPay.as("uP3"), $"uP0.shop_id" === $"uP3.shop_id" && $"uP3.date" === date_sub($"uP0.date", 1))
      .join(userPay.as("uP4"), $"uP0.shop_id" === $"uP4.shop_id" && $"uP4.date" === date_sub($"uP0.date", 1))
      .join(userPay.as("uP5"), $"uP0.shop_id" === $"uP5.shop_id" && $"uP5.date" === date_sub($"uP0.date", 1))
      .join(userPay.as("uP6"), $"uP0.shop_id" === $"uP6.shop_id" && $"uP6.date" === date_sub($"uP0.date", 1))
      .join(userPay.as("uP7"), $"uP0.shop_id" === $"uP7.shop_id" && $"uP7.date" === date_sub($"uP0.date", 1))
      .join(userPay.as("uP8"), $"uP0.shop_id" === $"uP8.shop_id" && $"uP8.date" === date_sub($"uP0.date", 1))
      .join(userPay.as("uP9"), $"uP0.shop_id" === $"uP9.shop_id" && $"uP9.date" === date_sub($"uP0.date", 1))
      .join(userPay.as("uP10"), $"uP0.shop_id" === $"uP10.shop_id" && $"uP10.date" === date_sub($"uP0.date", 1))
      .join(userPay.as("uP11"), $"uP0.shop_id" === $"uP11.shop_id" && $"uP11.date" === date_sub($"uP0.date", 1))
      .join(userPay.as("uP12"), $"uP0.shop_id" === $"uP12.shop_id" && $"uP12.date" === date_sub($"uP0.date", 1))
      .join(userPay.as("uP13"), $"uP0.shop_id" === $"uP13.shop_id" && $"uP13.date" === date_sub($"uP0.date", 1))
      .join(userPay.as("uP14"), $"uP0.shop_id" === $"uP14.shop_id" && $"uP14.date" === date_sub($"uP0.date", 1))
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
    userPay.show(10)
  }
}
