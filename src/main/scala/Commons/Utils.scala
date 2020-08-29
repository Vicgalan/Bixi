package Commons

import org.apache.spark.sql.DataFrame

class Utils {

  val spark = org.apache.spark.sql.SparkSession.builder
    .master("local[*]")
    .appName("BIXI")
    .getOrCreate()

  def readData(path: String): DataFrame = {
    spark.read.format("csv")
      .option("header", "True")
      .option("inferSchema","true")
      .load(path)
  }

}


