package com.ingestion

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
object GetdataExcel {

  def parseargs(args: Array[String]): Map[String,String] ={
    args.map( x=>{
      val tuple= x.split("==")
      (tuple(0).substring(1),tuple(1))}
    ).toMap
  }
  def main(args : Array[String]): Unit ={
    System.setProperty("hadoop.home.dir", "C:\\winutils-master\\winutils-master\\hadoop-2.7.1")
    val spark= SparkSession.builder().appName("synergy").master("local").getOrCreate()
    val opts=parseargs(args)
    val sheetname : String=if (opts("sheetname") == "SngyIssues" )  "Open Issues" else "Open Actions"
    print(sheetname)
    var Rec1=StructType(Array(
      StructField("Action_Plan_ID", StringType, true),
      StructField("Issue_ID", StringType, true),
      StructField("Issue_Audit_CM_ID", StringType, true),
      StructField("Action_Plan_Synergy_4_4_Action_ID", StringType, true),
      StructField("Action_Plan_Synergy_4_4_Issue_ID", StringType, true),
      StructField("Action_Plan_Synergy_4_4_Audit_ID", StringType, true),
      StructField("CM_Title", StringType, true),
      StructField("Audit_Title", StringType, true),
      StructField("Audit_Type", StringType, true),
      StructField("Audit_CE_Assessment", StringType, true),
      StructField("Issue_Title", StringType, true),
      StructField("Action_Plan_Title", StringType, true),
      StructField("Action_Plan_Description", StringType, true),
      StructField("Action_Plan_Expected_Completion_Date", StringType, true),
      StructField("Action_Plan_Revised_Expected_Completion", StringType, true),
      StructField("Status", StringType, true),
      StructField("Action_Plan_Flag", StringType, true),
      StructField("Action_Plan_Due_Status", StringType, true),
      StructField("Business_Action_Owner_1_Title", StringType, true),
      StructField("Business_Action_Owner_1_Name_Surname", StringType, true),
      StructField("Responsible_Business", StringType, true),
      StructField("Responsible_Sub_Business", StringType, true),
      StructField("Responsible_Function", StringType, true),
      StructField("Responsible_Sub-function", StringType, true),
      StructField("Responsible_Country", StringType, true),
      StructField("Internal_Audit_Team", StringType, true),
      StructField("Internal_Audit_Sub_Team", StringType, true),
      StructField("Action_Plan_Coordinator_Delimited", StringType, true)
    ))

    if (sheetname == "Open Issues")
    {

      Rec1 = StructType(Array(
        StructField("Issue_ID", StringType, true),
        StructField("Issue_Audit_CM_ID", StringType, true),
        StructField("Synergy_4_4_Issue_ID", StringType, true),
        StructField("Synergy_4_4_Audit_ID", StringType, true),
        StructField("Issue_Exco_Owner", StringType, true),
        StructField("Issue_Impacted_Area", StringType, true),
        StructField("CM_Title_Issue", StringType, true),
        StructField("Audit_Title", StringType, true),
        StructField("Audit_Type", StringType, true),
        StructField("Audit_CE_Assessment", StringType, true),
        StructField("Date_Created", StringType, true),
        StructField("Issue_Title", StringType, true),
        StructField("Issue_Description", StringType, true),
        StructField("Root_Cause", StringType, true),
        StructField("Primary_Key_Risk", StringType, true),
        StructField("Secondary_Key_Risk", StringType, true),
        StructField("Issue_Priority_Group", StringType, true),
        StructField("Issue_Priority_BU_Function_Country", StringType, true),
        StructField("Status", StringType, true),
        StructField("Audit_Issue_Flag", StringType, true),
        StructField("Issue_Due_Status", StringType, true),
        StructField("Agreed_Remediation_Timeframe_from_Issue_Created_Date", StringType, true),
        StructField("Timeframe_from_Issue_Created_Date_Issue_Age", StringType, true),
        StructField("Issue_Expected_Completion", StringType, true),
        StructField("Issue_Revised_Expected_Completion", StringType, true),
        StructField("Issue_Date_Re-Opened_at_IV", StringType, true),
        StructField("Overdue_Commentary", StringType, true),
        StructField("Reason_for_Issue_Re_Open", StringType, true),
        StructField("Issue_ReOpened_Commentary", StringType, true),
        StructField("Resp_Business_Exec_1_Name_Surname", StringType, true),
        StructField("Resp_Business_Exec_1_Title", StringType, true),
        StructField("Responsible_Business", StringType, true),
        StructField("Responsible_Sub_Business", StringType, true),
        StructField("Responsible_Function", StringType, true),
        StructField("Responsible_Sub-function", StringType, true),
        StructField("Responsible_Country", StringType, true),
        StructField("Impacted_Business", StringType, true),
        StructField("Impacted_Sub_Business", StringType, true),
        StructField("Impacted_Function", StringType, true),
        StructField("Impacted_Sub_function", StringType, true),
        StructField("Impacted_Country", StringType, true),
        StructField("Internal_Audit_Team", StringType, true),
        StructField("Internal_Audit_Sub_Team", StringType, true),
        StructField("Issue_Coordinators_Delimited", StringType, true),
        StructField("Tier_1_Legal_Regulated_Entities", StringType, true),
        StructField("Tier_2_Legal_Regulated_Entities", StringType, true)
      ))
    }

    // val sheetname =
    val landingpath=opts("landingpath")
    val rawpath=opts("rawpath")
    val tmpfile=opts("tmpfile")
    val synexcel=spark.read.format("com.crealytics.spark.excel").option("sheetName",sheetname).option("useHeader","true").option("multiLine", "true").option("treatEmptyValuesAsNulls", "true").option("addColorColumns", "false").option("inferSchema", "false").option("location",landingpath).option("userSchema","true").schema(Rec1).load()
    val rowcount=synexcel.count()
    synexcel.printSchema()
    synexcel.coalesce(1).write.mode("overwrite").option("header", "true").option("delimiter", "~").option("multiline", "True").parquet(rawpath)

    // synexcel.write.format("com.crealytics.spark.excel").option("useHeader","true").save(rawpath)
    print(rowcount)
    val info= CreateInfoFile.getInfoFile(rowcount,rowcount).toString
    val info1 = spark.sparkContext.parallelize(Seq(info)).repartition(1)
    info1.saveAsTextFile(s"$tmpfile")
    val hadoopConfig=spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(fs, new Path(s"$tmpfile"), fs, new Path(s"$rawpath/_INFO"), true, hadoopConfig, null)


  }
}
