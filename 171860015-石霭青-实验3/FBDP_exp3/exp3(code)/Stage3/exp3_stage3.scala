package oasans.com

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.StreamingContext._

object exp3_stage3 {

  def main(args: Array[String]): Unit = {

    val conf=new SparkConf().setAppName("exp3_stage3").setMaster("local")
    val sc=new SparkContext(conf)
    val lines=sc.textFile("hdfs://localhost:9000/input_exp3/million_user_log.csv")

    //第一小题
    val sum_groupbyprovince1 = lines.map(line => {
      val info =line.split(",")
      ((info(10),info(2), info(7)),1)
    }).filter(record => record._1._3 == "2").reduceByKey(_+_).groupBy(_._1._1).map(
      test => {
        val province = test._1

        val top10 = test._2.toList.sortBy(_._2)(Ordering.Int.reverse).take(10).
          map(item => item._1._2+"  "+item._2)

        (province, top10)
      }
    )
    //sum_groupbyprovince1.foreach(println)

    //第二小题
    val sum_groupbyprovince2 = lines.map(line => {
      val info =line.split(",")
      ((info(10),info(1), info(7)),1)
    }).filter(record => record._1._3 == "2").reduceByKey(_+_).groupBy(_._1._1).map(
      test => {
        val province = test._1

        val top10 = test._2.toList.sortBy(_._2)(Ordering.Int.reverse).take(10).
          map(item => item._1._2+"  "+item._2)

        (province, top10)
      }
    )
    //sum_groupbyprovince2.foreach(println)

    //第三小题,浏览行为相当于点击行为，action=0
    val count_click = lines.map(
      line => {
        val info = line.split(",")
        ((info(4),info(7)),1)
      }
    ).filter(record => record._1._2 == "0").reduceByKey(_+_).
      sortBy(_._2, false).take(10)

    //count_click.foreach(println)

  }
}
