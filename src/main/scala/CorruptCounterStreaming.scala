import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by davidsuarez on 3/03/16.
 */
object CorruptCounterStreaming extends App {

  val conf = new SparkConf().setMaster("local[2]").setAppName("CorruptCounterStreaming")
  val sc = new StreamingContext(conf, Seconds(10))

  val logFile = "src/main/resources"
  val file = sc.textFileStream(logFile)

  val corruptPaymentRDD = file.map(x => {
    val arr = x.split(",")
    new CorruptPayment(arr(0), Integer.parseInt(arr(1)), Integer.parseInt(arr(2)))
  })

  corruptPaymentRDD.map(x => (x.name, x.payment)).reduceByKey(_ + _)

  corruptPaymentRDD.print()

  sc.start()
  sc.awaitTermination()
}
