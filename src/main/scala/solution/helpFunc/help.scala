package solution.helpFunc

object help {

  // поиск достоверных данных
  // от менее достоверных. К самым достоверным
  // Other estimate
  // Provisional figure
  // Final figure, complete

  // Определение населения от менее предпочитаемого к самому предпочитаеому
  // Estimate - de jure  (граждане)
  // Estimate - de facto  (присутствующее) в день переписи
  // Sample survey - de facto
  // Census - de jure - complete tabulation
  // Census - de facto - complete tabulation

  def matchRecordType(recordType: String): Int = recordType match {
    case "\"Census - de facto - complete tabulation\"" => 5
    case "\"Census - de jure - complete tabulation\"" => 4
    case "\"Sample survey - de facto\"" => 3
    case "\"Estimate - de facto\"" => 2
    case "\"Estimate - de jure\"" => 1
    case _ => 0
  }

  def matchReliability(reliability: String): Int = reliability match {
    case "\"Final figure, complete\"" => 30
    case "\"Provisional figure\"" => 20
    case "\"Other estimate\"" => 10
    case _ => 0
  }

  def relevance(recordType: String, reliability: String): Int ={
    matchReliability(reliability) + matchRecordType(recordType)
  }


  def typeCity(typeC: String): Int = typeC match {
    case "\"Urban agglomeration\"" => 3
    case "\"City proper\"" => 2
    case _ => 1
  }
}
