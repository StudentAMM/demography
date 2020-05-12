package solution.domain


import org.apache.spark.sql.types._

object CityPopulation extends Enumeration {

  private val DELIMITER = ","

  val COUNTRY_OR_AREA, YEAR, AREA, SEX, CITY, CITY_TYPE, RECORD_TYPE, RELIABILITY, SOURCE_YEAR, VALUE, VALUE_FOOTNOTES = Value

  val structType = StructType(
    Seq(
      StructField(COUNTRY_OR_AREA.toString, StringType),
      StructField(YEAR.toString, IntegerType),
      StructField(AREA.toString, StringType),
      StructField(SEX.toString, StringType),
      StructField(CITY.toString, StringType),
      StructField(CITY_TYPE.toString, StringType),
      StructField(RECORD_TYPE.toString, StringType),
      StructField(RELIABILITY.toString, StringType),
      StructField(SOURCE_YEAR.toString, IntegerType),
      StructField(VALUE.toString, DoubleType),
      StructField(VALUE_FOOTNOTES.toString, StringType)
    )
  )

  def apply(row: String): CityPopulation = {
    val seq = row.split(DELIMITER).foldLeft[(Seq[String], String)](Nil, "") {
      case ((accumLines, accumString), newLine) =>
        val isInAnOpenString = accumString.nonEmpty
        val lineHasOddQuotes = newLine.count(_ == '"') % 2 == 1
        (isInAnOpenString, lineHasOddQuotes) match {
          case (true, true) => (accumLines :+ (accumString + DELIMITER + newLine)) -> ""
          case (true, false) => accumLines -> (accumString + DELIMITER + newLine)
          case (false, true) => accumLines -> newLine
          case (false, false) => (accumLines :+ newLine) -> ""
        }
    }._1 :+ ""
    CityPopulation(
      seq(COUNTRY_OR_AREA.id),
      seq(YEAR.id).toInt,
      seq(AREA.id),
      seq(SEX.id),
      seq(CITY.id),
      seq(CITY_TYPE.id),
      seq(RECORD_TYPE.id),
      seq(RELIABILITY.id),
      seq(SOURCE_YEAR.id).toInt,
      seq(VALUE.id).toDouble,
      seq(VALUE_FOOTNOTES.id)
    )
  }
}

case class CityPopulation(countryOrArea: String, year: Int, area: String, sex: String, city: String, cityType: String,
                          recordType: String, reliability: String, sourceYear: Int, value: Double, valueFootnotes: String)