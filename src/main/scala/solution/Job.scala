package solution

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import solution.domain.{CityPopulation, _}

// Необходимо реализовать job, рассчитывающий:
//  население стран,
//  для каждой страны:
//    o  количество городов-миллионников,
//    o  топ 5 самых крупных городов,
//    o  соотношение мужского и женского населения.

case class Result(uniqSex: Set[String], value: Seq[Double], sumPopulation: Double)


case class Result2(uniqSex: Seq[String], value: Seq[Double], reliability: Seq[String],
                   recordType: Seq[String], male: Double, female: Double)

object Job extends App {

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("lab_1")
  val sc = new SparkContext(sparkConf)
  val conf = new Configuration()
  val fs = FileSystem.get(conf)
  fs.delete(new Path(Parameters.output_path), true)

  // считывание данных
  val population = sc.textFile(Parameters.path_population)
    .map(CityPopulation(_))

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

  population.map(pp => ((pp.countryOrArea, pp.year, pp.city, pp.cityType, pp.recordType, pp.reliability, pp.sourceYear),
    (pp.sex, pp.value)))
    .aggregateByKey(Result(Set.empty[String], Seq.empty[Double], 0))((acc, pp) => {
      val sex = acc.uniqSex + pp._1
      val value = acc.value :+ pp._2
      Result(sex, value, acc.sumPopulation + pp._2)
    }, (one, two) => {
      if (two.uniqSex.last == "Male")
        Result(one.uniqSex,
          one.value ++ two.value,
          one.sumPopulation + two.sumPopulation)
      else
        Result(one.uniqSex,
          two.value ++ one.value,
          one.sumPopulation + two.sumPopulation)
    }).map { case (key, result) => ((key._1, key._2, key._3, key._4),
      // head - male    last - female  sumPopulation - both
    (key._5, key._6, result.value.head, result.value.last, result.sumPopulation))
  }
  // На данном этапе возможна такая ситауция. Нужно оставить самое точное значение(точность зависит от наших предпочтений)
  // ("Austria",2011,"Bregenz","City proper")	("Census - de jure - complete tabulation","Final figure, complete",13334.0,14497.0,27831.0)
  //("Austria",2011,"Bregenz","City proper")	("Estimate - de jure","Final figure, complete",13360.0,14424.0,27784.0)
  // ("Austria",2011,"Bregenz","City proper")	("Census - de facto - complete tabulation","Final figure, complete",13234.0,14497.0,27831.0)
  //("Austria",2011,"Bregenz","City proper")	("Estimate - de facto","Final figure, complete",13960.0,14424.0,27784.0)
    .groupByKey()
    .map{ case (key, stat) => (key, stat.maxBy( el =>helpFunc.help.relevance(el._1, el._2)))}
  // на данном этапе найдено наиболее актуальное число жителей в конкретном городе в конкретный год
    .map {
      case (key, result) =>
        s"$key\t$result"
    }.saveAsTextFile(Parameters.output_path)

}
