import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._

/**
  * Created by samgupta0 on 7/10/2018.
  */
object App2 {

  def main(args:Array[String]) ={


    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(4))
    //spark.

    ssc.checkpoint("./")

    val linesStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9091)

    val wordsStream: DStream[String] =  linesStream.flatMap(rdd => {

      rdd.split(",")
    })

    val pairs = wordsStream.map(x => (x, 1))
    val updatedPair = pairs.updateStateByKey[Int](updateFunction _)


    wordsStream.print()

    updatedPair.print()
//    val sumStream = pairs.reduceByKey (_ + _ )


      //.reduce(_ + _)

   // sumStream.print()
    ssc.start()
    ssc.awaitTermination()
   // sumStream.wait()
  }

  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    val newCount = newValues.foldLeft(runningCount.getOrElse(0))(_+_)

    Some(newCount)
  }
}
