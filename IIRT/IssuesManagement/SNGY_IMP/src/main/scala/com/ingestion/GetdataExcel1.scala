package com.ingestion

import org.apache.spark.sql.SparkSession
object GetdataExcel1 {

  def parseargs(args: Array[String]): Map[String,String] ={
    args.map( x=>{
      val tuple= x.split("==")
      (tuple(0).substring(1),tuple(1))}
    ).toMap
  }
  def main(args : Array[String]): Unit ={
    val spark= SparkSession.builder().appName("synergy").master("local").getOrCreate()
    val opts=parseargs(args)
    val sheetname : String=opts("sheetname")
    print(sheetname)
    val landingpath=opts("landingpath")
    val rawpath=opts("rawpath")
    val tmpfile=opts("tmpfile")
    val synexcel=spark.read.format("com.crealytics.spark.excel").option("sheetName","Sheet1").option("useHeader","true").option("multiLine", "true").option("treatEmptyValuesAsNulls", "true").option("addColorColumns", "false").option("inferSchema", "false").option("location",landingpath).load()
    val rowcount=synexcel.count()
    synexcel.printSchema()
    synexcel.show(10)
    synexcel.coalesce(1).write.mode("overwrite").option("header", "false").option("delimiter", "~").option("multiline", "True").parquet(rawpath)

    // synexcel.write.format("com.crealytics.spark.excel").option("useHeader","true").save(rawpath)
    print(rowcount)


  }
}
