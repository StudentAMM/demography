package solution

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import solution.domain.{CityPopulation, _}


case class Result(uniqSex: Set[String], value: Seq[Double], sumPopulation: Double)

object Job extends App {

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("lab_1")
  val sc = new SparkContext(sparkConf)
  val conf = new Configuration()
  val fs = FileSystem.get(conf)
  fs.delete(new Path(Parameters.output_path), true)
  val population = sc.textFile(Parameters.path_population)
    .map(CityPopulation(_))

  // Необходимо реализовать job, рассчитывающий:
  //  население стран,
  //  для каждой страны:
  //    o  количество городов-миллионников,
  //    o  топ 5 самых крупных городов,
  //    o  соотношение мужского и женского населения.

  population.map(pp => ((pp.countryOrArea, pp.year, pp.city, pp.cityType, pp.recordType, pp.sourceYear), (pp.sex, pp.value)))
    .aggregateByKey(Result(Set.empty[String], Seq.empty[Double], 0))((acc, pp) => {
      val uniq = acc.uniqSex + pp._1
      val uniqNum = acc.value :+ pp._2
      Result(uniq, uniqNum, acc.sumPopulation+pp._2)
    }, (one, two) => {
      if (two.uniqSex.last == "Male")
        Result(one.uniqSex,
          one.value ++ two.value,
          one.sumPopulation + two.sumPopulation)
      else
        Result(one.uniqSex,
          two.value ++ one.value,
          one.sumPopulation + two.sumPopulation)
    }).map{
    case (key, result) =>     // head - male    last - female  result - both
      //s"$key\t${result.value.size}\t${result.value.head\t${result.value.last}\t${result.sumPopulation}"
      s"$key\t${result.value.size}\t${result.value.head/result.value.last}\t${result.sumPopulation}"
  }.saveAsTextFile(Parameters.output_path)
}
