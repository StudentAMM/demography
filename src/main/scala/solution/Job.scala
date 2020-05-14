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

case class Result2(countCity: Int, city: Seq[(String, Double)])

case class Result3(countMale: Double, countFemale: Double, countBoth: Double)

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

  val temp = population.map(pp => ((pp.countryOrArea, pp.year, pp.city, pp.cityType, pp.recordType, pp.reliability, pp.sourceYear),
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
    .map { case (key, stat) => (key, stat.maxBy(el => helpFunc.help.relevance(el._1, el._2))) }
  // на данном этапе найдено наиболее актуальное число жителей в конкретном городе в конкретный год
  // теперь можно переходить к решению задачи

  // ((countryOrArea, year, ),    (city,  cityType, male,    female,  bouth))
  val statisticByCity = temp.map(j => ((j._1._1, j._1._2), (j._1._3, j._1._4, j._2._3, j._2._4, j._2._5)))
    .filter(g => g._2._2 == "\"City proper\"")
    .aggregateByKey(Result2(0, Seq.empty[(String, Double)]))((acc, pp) => {
      if (pp._5 > 1000000)
        Result2(acc.countCity + 1, acc.city :+ (pp._1, pp._5))
      else
        Result2(acc.countCity, acc.city :+ (pp._1, pp._5))
    }, (one, _) => {
      Result2(one.countCity, one.city)
    }).map { case (key, result) => ((key._1, key._2),
    (result.countCity, result.city.sortBy(f => f._2).reverse
      .take(5).map(el => el._1)))
  } // посчитано кол-во городов миллиоников и топ 5 городов


  val populationCountry = temp.map(j => ((j._1._1, j._1._2, j._1._3), (j._1._4, j._2._3, j._2._4, j._2._5)))
    .groupByKey()
    .map { case (key, stat) => (key, stat.maxBy(el => helpFunc.help.typeCity(el._1))) } // выбирал самые подходящие записи для подсчета общей численности населения стран
    .map(el => ((el._1._1, el._1._2), (el._2._2, el._2._3)))
    //.map{ case ((countryOrArea,year,_),(_, male, female, _)) => ((countryOrArea,year),(male, female))}
    .aggregateByKey(Result3(0, 0, 0))((acc, pp) => {
    val male = pp._1
    val female = pp._2
    Result3(acc.countMale + male, acc.countFemale + female, acc.countBoth + male + female)
  }, (one, _) => {
    Result3(one.countMale, one.countFemale, one.countBoth)
    // (countryOrArea,year,countMale,countFemale,countBoth)
  }).map(el=>(el._1._1, el._1._2, el._2.countMale/el._2.countFemale, el._2.countBoth))
    // подсчитано соотношение мужского и женского населения, а также общее население стран

   //для души подсчет населения мира :)
    populationCountry.map(el=>(el._2,el._4))
      .groupByKey()
      .map(el=>(el._1, el._2.sum))
      .map {
        case (key, result) =>
          s"$key\t$result"
      }.saveAsTextFile(Parameters.output_path)
}
