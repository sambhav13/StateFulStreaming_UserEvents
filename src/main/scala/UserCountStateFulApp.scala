import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._

import net.liftweb.json._
/**
  * Created by samgupta0 on 7/10/2018.
  */
object UserCountStateFulApp {
  case class User(id: String, name: String)

  def main(args:Array[String]) = {
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(4))
    //spark.

    ssc.checkpoint("./")

    val linesStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9091)


    val userEvents: DStream[User] = linesStream.map(rdd => {

      implicit val formats = DefaultFormats
      val jsonObj = parse(rdd).extract[User]
      jsonObj
    })
    /*val wordsStream: DStream[String] =  linesStream.flatMap(rdd => {

    rdd.split(",")
  })*/

    val pairs = userEvents.map(x => (x, 1))
    val updatedPair = pairs.updateStateByKey[Int](updateFunction _)


    userEvents.print()

    updatedPair.print()
    updatedPair.foreachRDD(rdd =>
      rdd.foreach(user => println("The user:" + user._1.name + " Count:" + user._2))
    )
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
