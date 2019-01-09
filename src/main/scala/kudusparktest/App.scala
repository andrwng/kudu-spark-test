package test

import org.apache.kudu.client.CreateTableOptions
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kudu.spark.kudu._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.JavaConverters._

case class SimpleRow(time: Long, value: String)

object TestMain {

  def createStreamingContext(args : Array[String]) : StreamingContext = {
    val conf = new SparkConf().setAppName("TestApp").setMaster("yarn")
    val sc = new SparkContext(conf)
    val streamingContext = new StreamingContext(sc, Seconds(1))

    // Create a queue of strings with which to create a DStream.
    val lines = Seq("Just a bunch of words", "What else do you want from me")
    val rdd: RDD[String] = sc.parallelize(lines)
    val queue = new mutable.Queue[RDD[String]]
    for (a <- 1 to 1000) {
      queue.enqueue(rdd)
    }
    val stream = streamingContext.queueStream(queue)

    // Create a table.
    val simpleSchema = StructType(
      StructField("time", LongType, false) ::
      StructField("value", StringType, true) :: Nil )
    val kuduPrimaryKey = Seq("time")
    val createTableOptions = new CreateTableOptions()
    createTableOptions.addHashPartitions(List("time").asJava, 3).setNumReplicas(3)
    val tableName = "table%1$d".format(System.currentTimeMillis() / 1000)

    val kuduContext = new KuduContext("awong-kerb-1.gce.cloudera.com", streamingContext.sparkContext)
    kuduContext.createTable(tableName, simpleSchema, kuduPrimaryKey, createTableOptions)

    // Iterate through the RDDs and insert it into Kudu.
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSession.implicits._
    stream.foreachRDD(rdd => {
      val newRDD: RDD[SimpleRow] = rdd.map(record => {
        SimpleRow(System.nanoTime(), record)
      })
      kuduContext.insertIgnoreRows(newRDD.toDF(), tableName)
    })
    streamingContext
  }

  def main(args: Array[String]): Unit = {
    val ssc = createStreamingContext(args)
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}