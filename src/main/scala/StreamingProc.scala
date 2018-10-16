import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent


/**
  * Created by opu on 2018/10/15.
  */
object StreamingProc {
  def main(args: Array[String]): Unit = {

    // streaming context configuration
    val spark = InitializeSparkSession()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics = Array("EbayMetric"), kafkaParams = getKafkaParams())
    )

    // parse the kafka byte stream to metric object, suppose json
    val signals: DStream[Signal] = stream.map(record => {
      EbaySchemaTool.parMetric(record.value())
    })

    val signal_group: DStream[Signal] = signals.groupBy(
      _.get("dimension").get("datacenter")).groupBy(
      _.get("dimension").get("application"))

    val groupDS: DStream[Signal] = s.filter(
      _.get("dimension").get("datacenter") == "slc"
    ).filter(
      _.get("dimension").get("application") == "search"
    )

    val searchcount: BigInt = groupDS.map(_ => {
      "searchCount": _.get("metrics").get("searchCount")
    }).aggregateByKeyAndWindow(
      (v1: Int, v2: Int) => v1 + v2, Seconds(60), Seconds(10)
    )
    searchcount.print() // may be write database
    // here give a example to send http post to user
    val url = "发送接口地址"
    val content_type = "application/x-www-form-urlencoded"
    try{
      val entity = new UrlEncodedFormEntity(searchcount,"UTF-8")
      entity.setContentType(content_type)
      HttpClientUtil.post(url, entity)
    }catch{
      case e:Throwable=>e.printStackTrace()
    }


    val cpuusage: Double = groupDS.map(_ => {
      "cpuUsage": _.get("metrics").get("cpuUsage")
    }).sortByKeyAndWindow(
      (v1: Int, v2: Int) => v1 + v2, Seconds(60), Seconds(10)
    ).take(0.05*groupDS.count())

    for (tuple <- cpuusage) {
      println("result : " + tuple)
    } // may be write database
    // here give a example to send http post to user
    try{
      val entity = new UrlEncodedFormEntity(cpuusage,"UTF-8")
      entity.setContentType(content_type)
      HttpClientUtil.post(url, entity)
    }catch{
      case e:Throwable=>e.printStackTrace()
    }

    ssc.start()
    ssc.awaitTermination()
  }


  def InitializeSparkSession(): SparkSession={
    SparkSession
      .builder()
      .appName("Realtime Processing")
      .config("spark.debug.maxToStringFields", "100")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/spark-warehouse")
      .enableHiveSupport()
      .getOrCreate();
  }

  def getKafkaParams(): Map[String, Object] = {
    Map[String, Object](
      "bootstrap.servers" -> "ipAddress1:port, ipAddress2:port, ipAddress3:port.....",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "consumer-group",
      "auto.offset.reset" -> "earliest ",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
  }

}
