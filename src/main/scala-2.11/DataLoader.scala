import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.joda.time.{DateTime, Period}

/**
  * Created by diego on 3/5/17.
  */
object DataLoader {
  val datasetPath: String = "/home/diego/IdeaProjects/untitled1/src/"
  var userPay: DataFrame = null
  var shopInfo: DataFrame = null
  var userView: DataFrame = null

  var spark: SparkSession = SparkSession
    .builder().master("local")
    .appName("Spark SQL basic example")
    .getOrCreate()

  def loadFile(filename: String): DataFrame = return spark.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").load(datasetPath.+(filename))


  def loadData(): Unit = {
    //Main Datasets
    userPay = (loadFile("user_pay.csv"))
    shopInfo = (loadFile("shop_info.csv"))
    userView = (loadFile("user_view.csv"))

    //spark.udf.register("avgPayPerShopPerDoW", avgPayPerShopPerDoW(_: String, _: Int, _: Int))

    userPay = userPay.select(
      functions.col("shop_id"),
      functions.col("user_id"),
      functions.month(functions.col("timestamp")).name("month"),
      functions.dayofmonth(functions.col("timestamp")).name("day"),
      functions.year(functions.col("timestamp")).name("year"),
      functions.date_format(functions.col("timestamp"), "EEEE").name("DoW"),
      functions.date_format(functions.col("timestamp"), "yyyy-MM-dd").name("date")
    ).groupBy(
      functions.col("shop_id"),
      functions.col("year"),
      functions.col("month"),
      functions.col("day"),
      functions.col("DoW"),
      functions.col("date")
    ).count().withColumnRenamed("count", "payments")

    userView = userView.select(
      functions.col("shop_id"),
      functions.col("user_id"),
      functions.month(functions.col("timestamp")).name("month"),
      functions.dayofmonth(functions.col("timestamp")).name("day"),
      functions.year(functions.col("timestamp")).name("year"),
      functions.date_format(functions.col("timestamp"), "EEEE").name("DoW"),
      functions.date_format(functions.col("timestamp"), "yyyy-MM-dd").name("date")
    ).groupBy(
      functions.col("shop_id"),
      functions.col("year"),
      functions.col("month"),
      functions.col("day"),
      functions.col("DoW"),
      functions.col("date")
    ).count().withColumnRenamed("count", "views")

    var validShops = userPay.groupBy("shop_id").agg(min(col("date")),max(col("date"))).filter("datediff(max(date),min(date)) > 120").select("shop_id").cache()

    userPay = validShops.join(userPay,"shop_id").persist(StorageLevel.MEMORY_AND_DISK)
    userView = validShops.join(userView,"shop_id").persist(StorageLevel.MEMORY_AND_DISK)
    shopInfo = validShops.join(shopInfo,"shop_id").persist(StorageLevel.MEMORY_AND_DISK)


  }
  

  val avgPerDoW = functions.udf(
    (date: String, shop_id: Int, DoW: Int, nameCol: String, dataframe:DataFrame) => {
      var tmp = dataframe.filter(Predef.augmentString("shop_id = %i and date < unix_timestamp('%s')  and date > date_sub(unix_timestamp('%s'), 180) and DoW = %i").format(shop_id, date, date, DoW)).cache()
      val max: DateTime = DateTime.parse(tmp.agg(Tuple2.apply("date", "max")).map((x: Row) => x.getString(0)).first().toString())
      val min: DateTime = DateTime.parse(tmp.agg(Tuple2.apply("date", "min")).map((x: Row) => x.getString(0)).first().toString())
      var numOfDoW: Int = if ( min.dayOfWeek() != DoW && max.dayOfWeek() != DoW ) 0 else 1
      numOfDoW = numOfDoW + ((new Period(min, max)).getDays / 7)
      tmp.agg((nameCol, "sum")).first().getInt(0) / numOfDoW.toDouble
    }
  )

  val dailyAvgInAMonth = functions.udf(
    (date: String, shop_id: Int, month: Int, nameCol: String, dataframe:DataFrame) => {
      var tmp = dataframe.filter(Predef.augmentString("shop_id = %i and date < date_sub(unix_timestamp('%s'), " + month *30 + ")  and date > date_sub(unix_timestamp('%s'), " + (month + 1) *30 + ")").format(shop_id, date, date))
      tmp.agg((nameCol, "sum")).first().getInt(0) / 30.0
    }
  )



  def assembleDataset(): Unit = {
    var pay: _root_.org.apache.spark.sql.DataFrame = spark.sql("select shop_id, day, month, year, DoW, unix_timestamp(concat(year,'-',month,'-',day,' 00:00:00'), 'yyyy-MM-dd hh:mm:ss') as date, count(*) as payments from (select shop_id, user_id, MONTH(timestamp) as month, DAY(timestamp) as day, YEAR(timestamp) as year, date_format(timestamp, 'EEEE') as DoW from global_temp.userPay) group by shop_id, year, month, day,DoW")
    pay.createOrReplaceTempView("userPay")
    var view: _root_.org.apache.spark.sql.DataFrame = spark.sql("select shop_id, day, month, year, count(*) as views from (select shop_id, user_id, MONTH(timestamp) as month, DAY(timestamp) as day, YEAR(timestamp) as year from global_temp.userView) group by shop_id, year, month, day")
    view.createOrReplaceTempView("userView")
    var dataset: _root_.org.apache.spark.sql.DataFrame = spark.sql("select userView.shop_id, userView.year, userView.month, userView.day, userPay.DoW, userPay.date, userView.views, userPay.payments from userView inner join userPay on userView.shop_id=userPay.shop_id and userView.year=userPay.year and userView.month=userPay.month and userView.day=userPay.day")
    dataset.createOrReplaceTempView("dataset")
    dataset = spark.sql("select dataset.*, shopInfo.city_name, shopInfo.location_id, shopInfo.per_pay, shopInfo.score, shopInfo.comment_cnt, shopInfo.shop_level, shopInfo.category1, shopInfo.category2, shopInfo.category3 from dataset left join global_temp.shopInfo on dataset.shop_id=shopInfo.shop_id")
    dataset.createOrReplaceTempView("dataset")

    saveDataset(dataset, "dataset.csv")
  }

  def addAdditionalFields(): Unit = {
    loadFile("dataset.csv")
    var dateList: _root_.org.apache.spark.sql.DataFrame = spark.sql("select distinct dataset.date from global_temp.dataset where dataset.date is not null")
    spark.sql("select 0 as lastviews, 0 as shop_id, 0 as year, 0 as month, 0 as day from global_temp.dataset where 0=1").createOrReplaceTempView("partdataset")
    val iterator: _root_.java.util.Iterator[_root_.org.apache.spark.sql.Row] = dateList.toLocalIterator()
    while (iterator.hasNext()) {
      var row: Row = iterator.next()
      var partdataset: _root_.org.apache.spark.sql.DataFrame = spark.sql("( select count(*) as lastviews, shop_id, year, month, day from ( select p3.*, year(p3.timestamp) as year, month(p3.timestamp) as month, day(p3.timestamp) as day from global_temp.userView as p3 where p3.timestamp <= date_sub(from_unixtime(".+(row.get(0)).+("),1) and p3.timestamp >= date_sub(from_unixtime(").+(row.get(0)).+("),15) ) as p4 group by p4.shop_id, p4.year, p4.month, p4.day ) union (select distinct * from partdataset)"))
      partdataset.createOrReplaceTempView("partdataset")
    }

    //dataset = spark.sql("select dataset.*, shopInfo.city_name, shopInfo.location_id, shopInfo.per_pay, shopInfo.score, shopInfo.comment_cnt, shopInfo.shop_level, shopInfo.category1, shopInfo.category2, shopInfo.category3 from dataset left join global_temp.shopInfo on dataset.shop_id=shopInfo.shop_id")
    //dataset.createOrReplaceTempView("dataset")

  }

  def saveDataset(df: DataFrame, filename: String): Unit = {
    df.coalesce(1).write.format("csv").option("header", "true").save(datasetPath.+(filename))
  }
}
