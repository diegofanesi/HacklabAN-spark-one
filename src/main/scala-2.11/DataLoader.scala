import java.util.stream.Collectors

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.Encoders._
import org.joda.time.{DateTime, Period}
import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
  * Created by diego on 3/5/17.
  */
object DataLoader {
  val datasetPath: String = "/home/diego/IdeaProjects/untitled1/src/"
  var userPay: DataFrame = null
  var shopInfo: DataFrame = null
  var userView: DataFrame = null

  Logger.getLogger("org").setLevel(Level.OFF)

  val spark: SparkSession = SparkSession
    .builder().master("local[8]")
    .appName("Spark SQL basic example")
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
    ).count().withColumnRenamed("count", "payments")

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
    ).count().withColumnRenamed("count", "views")

    var validShops = userPay.groupBy("shop_id").agg(min(col("date")),max(col("date"))).withColumnRenamed("max(date)","max").withColumnRenamed("min(date)","min").filter("datediff(max,min) > 120").select("shop_id").cache()
    var validShopIds = validShops.select("shop_id").map { case Row(shop_id: Int) => shop_id }.collectAsList().toArray

    userPay = userPay.filter(col("shop_id").isin(validShopIds:_*)).persist(StorageLevel.MEMORY_AND_DISK)
    userView = userView.filter(col("shop_id").isin(validShopIds:_*)).persist(StorageLevel.MEMORY_AND_DISK)
    shopInfo = shopInfo.filter(col("shop_id").isin(validShopIds:_*)).persist(StorageLevel.MEMORY_AND_DISK)

    userPay = userPay.join(validShops, userPay.col("shop_id"), "inner").filter(datediff(col("date"), col("min")) > 90).select(
      userPay.col("shop_id"),
      col("year"),
      col("month"),
      col("day"),
      col("DoW"),
      col("date"),
      col("payments"),
      avgPerDoW("payments", userPay)(col("date"), col("shop_id"), col("DoW")),
      avgPerDoW("views", userView)(col("date"), col("shop_id"), col("DoW")),
      dailyAvgInAMonth("payments", userPay, 1)(col("date"),col("shop_id")),
      dailyAvgInAMonth("payments", userPay, 2)(col("date"),col("shop_id")),
      dailyAvgInAMonth("payments", userPay, 3)(col("date"),col("shop_id")),
      dailyAvgInAMonth("views", userView, 1)(col("date"),col("shop_id")),
      dailyAvgInAMonth("views", userView, 2)(col("date"),col("shop_id")),
      dailyAvgInAMonth("views", userView, 3)(col("date"),col("shop_id"))
    )
  }


  def avgPerDoW(nameCol: String, dataframe:DataFrame) = udf(
    (date: String, shop_id: Int, DoW: Int) => {
      var tmp = dataframe.filter(Predef.augmentString("shop_id = %i and date < unix_timestamp('%s')  and date > date_sub(unix_timestamp('%s'), 180) and DoW = %i").format(shop_id, date, date, DoW)).cache()
      val max: DateTime = DateTime.parse(tmp.agg(("date", "max")).map { case Row(dt: String) => dt }.first().toString())
      val min: DateTime = DateTime.parse(tmp.agg(("date", "min")).map { case Row(dt: String) => dt }.first().toString())
      var numOfDoW: Int = if ( min.dayOfWeek() != DoW && max.dayOfWeek() != DoW ) 0 else 1
      numOfDoW = numOfDoW + ((new Period(min, max)).getDays / 7)
      tmp.agg((nameCol, "sum")).first().getInt(0) / numOfDoW.toDouble
    }
  )

  def dailyAvgInAMonth(nameCol: String, dataframe:DataFrame, month: Int) = udf(
    (date: String, shop_id: Int) => {
      var tmp = dataframe.filter(Predef.augmentString("shop_id = %i and date < date_sub(unix_timestamp('%s'), " + month *30 + ")  and date > date_sub(unix_timestamp('%s'), " + (month + 1) *30 + ")").format(shop_id, date, date))
      tmp.agg((nameCol, "sum")).first().getInt(0) / 30.0
    }
  )

  def saveDataset(df: DataFrame, filename: String): Unit = {
    df.coalesce(1).write.format("csv").option("header", "true").save(datasetPath.+(filename))
  }

  def main(args: Array[String]): Unit = {
    loadData()
    userPay.show(10)
  }
}
