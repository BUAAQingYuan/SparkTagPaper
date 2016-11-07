package cn.buaaqingyuan.Sql

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by PC-LiNing on 2016/11/7.
  */
object ReadSql {

  def main(args:Array[String]){

    val conf = new SparkConf().setJars(List("/home/hadoop/SparkTagPaper.jar","/home/hadoop/mysql-connector-java-5.1.35.jar"))
    val sc = new SparkContext(conf)

    val jdbcurl = "jdbc:mysql://10.2.1.57:3306/mixtest"
    val username = "root"
    val password = "1993"

    val rdd = new JdbcRDD(
      sc,
      () => {
        Class.forName("com.mysql.jdbc.Driver").newInstance()
        DriverManager.getConnection(jdbcurl,username,password)
      },
      "SELECT scholarname from trans2 where id >= ? AND id <= ?",
      1,10,1,
      r => (r.getString(1))
    )

    rdd.collect().foreach(println)

    sc.stop()

  }

}
