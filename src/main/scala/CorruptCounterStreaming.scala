import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by davidsuarez on 3/03/16.
 */
object CorruptCounterStreaming extends App {

  val conf = new SparkConf().setMaster("local[2]").setAppName("CorruptCounterStreaming")
  val sc = new StreamingContext(conf, Seconds(3))

  val logFile = "src/main/resources/papers.csv"
  val file = sc.textFileStream(logFile)

  file.print()

}
