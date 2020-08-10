package com.ingestion

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession


object IMP_CNTR {
  private val log: Logger = LogManager.getLogger(this.getClass)
  def parseCmdLineArgs(args: Array[String]): Map[String, String] = {
    println(args mkString "\n")
    args.map(arg => {
      val parts = arg.split("==")
      (parts(0).substring(1), parts(1))
    }).toMap
  }
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils-master\\winutils-master\\hadoop-2.7.1")
    val opts = parseCmdLineArgs(args)
    val connectionurl: String = opts("connectionurl")
    val tablename: String = opts("tablename")
    val outputfilename: String = opts("outputfilename")
    val username: String = opts("username")
    val pwd: String = opts("pwd")
    val outinfo: String = opts("tmpfile")
    var filerowcount: Long=0
    val spark = SparkSession.builder().appName("IDP_Actions_Breach_BA1").master("local").getOrCreate()
    spark.conf.set("spark.sql.parquet.writeLegacyFormat","true")
    val colums: String = TablesandColumns.getcolumns(tablename).toString
    val mf = spark.read.format("jdbc").option("quote", "\"").option("multiLine", "true").option("wholeFile", "True").option("parserLib", "UNIVOCITY").
      option("driver","oracle.jdbc.driver.OracleDriver").option("url", connectionurl).
      option("dbtable", colums).option("user", username).option("password", pwd).load()
    val sourcerowcount = mf.count()

    if (sourcerowcount > 0) {
      mf.coalesce(1).write.mode("overwrite").option("header", "false").option("delimiter", "~").option("multiline", "True").option("quote", "\"").parquet(outputfilename)
      filerowcount = spark.read.parquet(outputfilename).count()
    }
    else
    {
      filerowcount=sourcerowcount
    }
    val info= CreateInfoFile.getInfoFile(sourcerowcount,filerowcount).toString
    val info1 = spark.sparkContext.parallelize(Seq(info)).repartition(1)
    info1.saveAsTextFile(s"$outinfo")
    val hadoopConfig=spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(fs, new Path(s"$outinfo"), fs, new Path(s"$outputfilename/_INFO"), true, hadoopConfig, null)

  }
}
