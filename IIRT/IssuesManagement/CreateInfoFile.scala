package com.ingestion

import java.util.Calendar

object CreateInfoFile {

  def getInfoFile(ctrlreccount: Long,filerowcount: Long): String ={
    val soureApplication = "IDP"
    val country = "ZA"
    val historyType = "Snapshot"
    val fileName = "IdpSystems"
    val sourceType = "Golden"
    val version = 1
    val recordCountColumn = "*"
    val t = "{" + "\n\n" + "  \"" + "metadata" + "\"" + ": {" + "\n" + "    \"" +
      "sourceApplication" + "\"" + ": " + "\"" + soureApplication + "\"" + ",\n" + "    \"" +
      "country" + "\"" + ": " + "\"" + country + "\"" + ",\n" + "    \"" +
      "historyType" + "\"" + ": " + "\"" + historyType + "\"" + ",\n" + "    \"" +
      "dataFilename" + "\"" + ": " + "\"" + fileName + "\"" + ",\n" + "    \"" +
      "sourceType" + "\"" + ": " + "\"" + sourceType + "\"" + ",\n" + "    \"" +
      "version" + "\"" + ": " + version + ",\n" + "    \"" +
      "informationDate" + "\"" + ": " + "\"" + Calendar.getInstance ().getTime () + "\"" + ",\n" + "    \"" +
      "additionalInfo" + "\": {}" + "\n  },\n\n" + "  \"" +
      "checkpoints" + "\"" + ": [\n\n    {\n" + "      \"" +
      "name" + "\"" + ": " + "\"" + "Source" + "\"" + ",\n" + "      \"" +
      "processStartTime" + "\"" + ": " + "\"" + Calendar.getInstance ().getTime () + "\"" + ",\n" + "      \"" +
      "processEndTime" + "\"" + ": " + "\"" + Calendar.getInstance ().getTime () + "\"" + ",\n" + "      \"" +
      "workflowName" + "\"" + ": " + "\"" + "Source" + "\"" + ",\n" + "      \"" +
      "order" + "\"" + ": 1" + ",\n" + "      \"" +
      "controls" + "\": [" + "\n        {\n          \"" +
      "controlName" + "\": \"recordcount\"," + "\n          \"" +
      "controlType" + "\": \"controlType.count\"," + "\n          \"" +
      "controlCol" + "\": \"" + recordCountColumn + "\",\n          \"" +
      "controlValue" + "\": " + ctrlreccount + "       }\n ]\n    },\n      {\n      \"" +
      "name" + "\"" + ": " + "\"" + "Raw" + "\",\n      \"" +
      "processStartTime" + "\"" + ": " + "\"" + Calendar.getInstance ().getTime () + "\",\n      \"" +
      "processEndTime" + "\"" + ": " + "\"" + Calendar.getInstance ().getTime () + "\",\n      \"" +
      "workflowName" + "\"" + ": " + "\"" + "Raw" + "\",\n      \"" +
      "order" + "\"" + ": 2" + ",\n" + "      \"" +
      "controls" + "\": [" + "\n        {\n          \"" +
      "controlName" + "\": \"recordcount\"," + "\n          \"" +
      "controlType" + "\": \"controlType.count\"," + "\n          \"" +
      "controlCol" + "\": \"" + recordCountColumn + "\",\n          \"" +
      "controlValue" + "\": " + filerowcount + "       }\n  ]\n    }\n\n  ]\n\n}"
    //  private val singleStringColumnDF = spark.sparkContext.parallelize(List("987987", "example", "example", "another example")).toDF
    return t
  }
}