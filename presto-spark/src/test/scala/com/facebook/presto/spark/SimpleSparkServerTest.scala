package com.facebook.presto.spark

import com.facebook.presto.execution.{ScheduledSplit, TaskSource}
import org.json4s._

import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.json4s.jackson.Serialization.read
import io.airlift.json.{JsonCodec, JsonModule}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


import scala.collection.JavaConverters._

class SimpleSparkServerTest extends POSUnitTestSpec {

  val master = "local[2]"
  val appName = "POS-Test"

  val conf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)
    .set("spark.driver.host", "localhost")
  var sc: SparkContext = _
  var hc: SQLContext = _

  override def beforeAll() {
    sc = new SparkContext(conf)
    hc = new SQLContext(sc)
  }

  override def afterAll() {
    if (sc != null) {
      while (!sc.isStopped) {
        sc.stop()
      }
      sc = null
    }
  }

//  def createDF(): DataFrame = {
//    val rddQueries = sc.parallelize(
//      Seq(
//        Row("2017-07-01", 1, "CRM"),
//        Row("2017-07-01", 1, "ADL"),
//        Row("2017-07-02", 1, "ADS"),
//        Row("2017-07-02", 1, "ADC"),
//        Row("2017-07-03", 1, "ADD"),
//        Row("2017-07-03", 1, "ADE")
//      ))
//
//    val df = hc.createDataFrame(
//      rddQueries,
//      StructType(
//        Array(
//          StructField("ds", StringType, false),
//          StructField("executionTime", IntegerType, false),
//          StructField("client", StringType, false)
//        ))
//    )
//    df.createOrReplaceTempView("test")
//    df
//  }


  it should "test print splits" in {
    val queryRunner = POSQueryRunner.createLocalQueryRunner()
    val sql = "select *  from tpch.sf1.lineitem"
    val taskSourceCodec = JsonCodec.jsonCodec(classOf[TaskSource])
    implicit val formats = DefaultFormats

    val plan = queryRunner.getLocalExecutionPlan(sql)
    val sources = queryRunner.getTaskSources(sql)

    val holderBcast = sc.broadcast(new MapperHolder())


    val sourcesJson = sources.asScala.map(source => write(sources))
    sc.parallelize(sourcesJson).foreach(x => print("Sources:" + x))

  }


}
