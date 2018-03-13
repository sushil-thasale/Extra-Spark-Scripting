import java.nio.file.Paths

/** Main class */
object WeatherDataAnalysis {

  import org.apache.spark.sql.SparkSession

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Time Usage")
      .config("spark.master", "local")
      .getOrCreate()

  /** Main function */
  def main(args: Array[String]): Unit = {
    val weatherData = spark.sparkContext.textFile(fsPath("1763.csv"))
      .map(extractData)
      .cache()

    val tminData = weatherData.filter(row => row._2._1 == "TMIN").map(row => (row._1, row._2._2))

    val tmaxData = weatherData.filter(row => row._2._1 == "TMAX").map(row => (row._1, row._2._2))

    val tminResult = tminData.groupByKey().mapValues(vals => vals.toList.sum/vals.toList.size)

    val tmaxResult = tmaxData.groupByKey().mapValues(vals => vals.toList.sum/vals.toList.size)

    val finalResult = tminResult.join(tmaxResult).collect()

    val finalResult2 = spark.sparkContext.textFile(fsPath("1763.csv"))
      .map(extractData2)
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4))
      .map(row => (row._1, row._2._2 / row._2._1, row._2._4 / row._2._3))
      .collect()

//    finalResult2.foreach(println)
  }

  /** @return The filesystem path of the given resource */
  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

  def extractData(line: String): (String, (String, Double)) = {
    val parts = line.split(",")
    val stationId = parts(0)
    val tempType = parts(2)
    val temperature = parts(3).toDouble
    (stationId, (tempType, temperature))
  }

  def extractData2(line: String): (String, (Long, Double, Long, Double)) = {
    val parts = line.split(",");
    val stationId = parts(0);
    val tempType = parts(2);
    val temperature = parts(3).toDouble;

    if(tempType == "TMIN") (stationId, (1, temperature, 0, 0));
    else (stationId, (0, 0, 1, temperature));
  }
}