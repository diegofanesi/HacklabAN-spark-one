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

  Logger.getLogger("org").setLevel(Level.OFF)

  val spark: SparkSession = SparkSession
    .builder().master("local[8]") //.master("spark://192.168.1.145:7077")
    .appName("Spark SQL basic example")
    .config("spark.executor.memory", "5g")
    //.config("spark.executor.cores","16")
    .getOrCreate()

  import spark.implicits._

  def loadFile(filename: String): DataFrame = return spark.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").load(datasetPath.+(filename))


  def loadData(): Unit = {
    //Main Datasets
    userPay = (loadFile("user_pay.csv"))
    shopInfo = (loadFile("shop_info.csv"))
    userView = (loadFile("user_view.csv"))


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

    var validShops = userPay.groupBy("shop_id").agg(min(col("date")), max(col("date"))).withColumnRenamed("max(date)", "max").withColumnRenamed("min(date)", "min").filter("datediff(max,min) > 120").persist(StorageLevel.MEMORY_ONLY)

    userPay.show(10)


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


  def getDataFromHistory(dataframe: DataFrame) = udf(
    (date: Date, shop_id: Int) => if (dataframe.filter($"shop_id" === shop_id && $"date" == date).count() == 0) 0 else dataframe.filter($"shop_id" === shop_id && $"date" == date).select($"nevents").first().getInt(0)
  )

  def saveDataset(df: DataFrame, filename: String): Unit = {
    df.coalesce(1).write.format("csv").option("header", "true").save(datasetPath.+(filename))
  }

  def main(args: Array[String]): Unit = {
    loadData()
    userPay.show(10)
  }
}
