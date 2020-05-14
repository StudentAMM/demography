package solution.helpFunc

object help {

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
}
