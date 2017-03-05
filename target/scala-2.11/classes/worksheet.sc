import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.storage.StorageLevel
import org.joda.time.{DateTime, Period}

val datasetPath: String = "/root/dataset/"

var spark: SparkSession = SparkSession
  .builder().master("local")
  .appName("Spark SQL basic example")
  .getOrCreate()

def loadFile(filename: String): DataFrame = return spark.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").load(datasetPath.+(filename))


//Main Datasets
var userPay=loadFile("user_pay.csv")
var shopInfo=(loadFile("shop_info.csv"))
var userView=(loadFile("user_view.csv"))

//spark.udf.register("avgPayPerShopPerDoW", avgPayPerShopPerDoW(_: String, _: Int, _: Int))

userPay=(userPay.select(
  functions.col("shop_id"),
  functions.col("user_id"),
  functions.month(functions.col("timestamp")).name("month"),
  functions.dayofmonth(functions.col("timestamp")).name("day"),
  functions.year(functions.col("timestamp")).name("year"),
  functions.date_format(functions.col("timestamp"), "EEEE").name("DoW")
).groupBy(
  functions.col("shop_id"),
  functions.col("year"),
  functions.col("month"),
  functions.col("day"),
  functions.col("DoW"),
  functions.unix_timestamp(functions.concat_ws("-", functions.col("year"), functions.col("month"), functions.col("year")), "yyyy-MM-dd hh:mm:ss").name("date")
).count().withColumnRenamed("count", "payments").persist(StorageLevel.MEMORY_AND_DISK))

userPay.show(10)
