package solution

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import solution.domain.{CityPopulation, _}

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

}
