package tools

import org.apache.spark.sql.SparkSession
/*
* Parquert schema to Hive create table DDL converter
* */
object DDLConverter extends App {
  val spark: SparkSession = SparkSession.builder().appName("DDLConverter").config("spark.master", "local").getOrCreate()
  println(ddlForParquet("/Users/kate/Downloads/Archive/part-00000.parquet", "test"))

  def ddlForParquet(path: String, tableName: String): String = {
    val dataFrame = spark.read.parquet(path)
    val columns = dataFrame.schema.map { field =>
      "  " + field.name + " " + field.dataType.catalogString.toUpperCase
    }

    s"""CREATE EXTERNAL TABLE $tableName (\n${columns.mkString(",\n")}\n)
       |STORED AS PARQUET
       |LOCATION 'file:${path.substring(0, path.lastIndexOf('/')+1)}';""".stripMargin
  }

}

