package com.ingestion
import java.io.{FileOutputStream, PrintWriter}
import java.nio.file.{Files, Paths}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import scala.io.Source
object JsonSchemaforIMP {
  def write(content: String, fileName: String): Unit = {
    val writer = new PrintWriter(new FileOutputStream(fileName), true)
    writer.println(content)
    writer.close()
  }
  def getSchema(path: String): List[(String, String, Int, Int)] = {
    val sourceSchema = Source.fromFile(path).getLines()
    var tupleList: List[(String, String, Int, Int)] = List()
    sourceSchema foreach ((line: String) => {
      val parts = line.split(",")
      tupleList = tupleList :+ (parts(0), parts(1), parts(2).toInt, parts(3).toInt)
    })
    tupleList
  }

  def parseCmdLineArgs(args: Array[String]): Map[String, String] = {
    println(args mkString "\n")
    args.map(arg => {
      val parts = arg.split("=")
      (parts(0).substring(1), parts(1))
    }).toMap
  }
  def main(args: Array[String]): Unit = {

    val opts = parseCmdLineArgs(args)

    val spark=SparkSession.builder().appName("JsonSchemaforIMP").master("local").getOrCreate()
    val schema = getSchema("C:\\Users\\ab006gy\\Desktop\\IDP\\ExportTemplate_IssueManagement.txt")
    val schemaStruct: Array[StructField] = schema.map(x => {
      StructField(
        x._1,
        x._2 match {
          case "string" => StringType
          //case "number" => if (x._4 != 0) DecimalType(x._3, x._4) else if (x._3 >= 10) LongType else if (x._3 <= 4) ShortType else IntegerType
          case "number" => if (x._4 != 0) DecimalType(x._3, x._4) else if (x._3 >= 10) LongType else IntegerType
        },
        nullable = true,
        metadata = if (x._2 matches "string") new MetadataBuilder().putString("length", x._3.toString).build() else Metadata.empty
      )
    }).toArray
    val structSchema = new StructType(schemaStruct)

    val df = spark.read
      .option("header", "false")
      .option("delimiter", "~")
      .schema(structSchema)
      .parquet("C:\\Users\\ab006gy\\Desktop\\IDP\\IDP1")


    val newStructType = df.schema
    //  var dateColumns = new util.HashSet[String]()
    //dateColumns.add("")
    val updatedStructTypeSeq = newStructType map ((sf: StructField) => {
      //if (dateColumns.contains(sf.name)) {
      //  println("Data type changed for the field " + sf.name)
      //  StructField(sf.name, DateType, sf.nullable, new MetadataBuilder().putString("pattern", opts("sourceDatePattern")).build())
      // } else {
      println("Data type NOT changed for the field " + sf.name)
      sf
      //  }
    })


    val schemaJson = new StructType(updatedStructTypeSeq.toArray).prettyJson.replace("#", "")//.replace("type\" : \"short", "type\" : \"smallint")
    Files.write(Paths.get("C:\\Users\\ab006gy\\Desktop\\IDP\\ExportTemplate_IssueManagement.json"), schemaJson.getBytes)
  }}
