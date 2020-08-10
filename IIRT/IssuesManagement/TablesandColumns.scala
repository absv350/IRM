package com.ingestion

object TablesandColumns {
  var columns = ""
  def getcolumns(tablename:String): String ={

    print(tablename)

    if (tablename == "ExportTemplate_IssueManagement") {
      columns = "(select Issue_ID,[Historic Ref number] Historic_Ref_number,cast(Issue_Creation_Date as varchar) Issue_Creation_Date,Issue_Type,Issue_Description,Issue_Priority,Issue_Status,Issue_Owner,cast(Issue_Due_Date as varchar) Issue_Due_Date,[Business Area] Business_Area,[Impacted Business_Function] Impacted_Business_Function,[Responsible Business_Function] Responsible_Business_Function,Country,Risk,Financial_Crime_Process,ActionComments,Action_ID,Action_Owner,cast(Action_Due_Date as varchar) Action_Due_Date,Action_Status,Action_Description,[Date Status Changed] Date_Status_Changed,Action_ID_Reference from vw_ExportTemplate_IssueManagement) as tmp"

     }

    else{

    }
    return columns
  }
}
