package Development

import Commons.Utils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.types.TimestampType


class yearAnalysis(utils: Utils, year: String) {

  import utils.spark.implicits._

  val pathOD = "src/data/OD/OD_" + year + ".csv"
  val pathStations = "src/data/Stations/Stations_" + year + ".csv"

  def histogram() {

    val ODByMonth = utils.readData(pathOD)
        .withColumn("month",substring($"start_date",6,2))
        .groupBy("month").agg(sum("duration_sec"))
        .withColumn("duration_hour", $"sum(duration_sec)" / 3600 )
        .orderBy("month")
        .select("month","duration_hour")

    ODByMonth.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .mode("overwrite")
      .save("src/results/ODByMonth" + year)

  }

  def topStations(){

    val stations = utils.readData(pathStations)
      .select("code","name")

    val totalStartStations = utils.readData(pathOD)
      .groupBy("start_station_code").count()
      .withColumnRenamed("count","total_start")
      .withColumnRenamed("start_station_code","code")
      .orderBy(desc("total_start"))
      .join(stations, "code")

    totalStartStations.coalesce(1).limit(10)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .mode("overwrite")
      .save("src/results/totalStartStations" + year)

    val totalEndStations = utils.readData(pathOD)
      .groupBy("end_station_code").count()
      .withColumnRenamed("count","total_end")
      .withColumnRenamed("end_station_code","code")
      .orderBy(desc("total_end"))
      .join(stations, "code")

    totalEndStations.coalesce(1).limit(10)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .mode("overwrite")
      .save("src/results/totalEndStations" + year)

    val totalInGeneral = totalEndStations.join(totalStartStations, Seq("code","name"))
      .withColumn("total", $"total_end" + $"total_start")
      .orderBy(desc("total"))

    totalInGeneral.coalesce(1).limit(10)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .mode("overwrite")
      .save("src/results/totalInGeneral" + year)

  }

  def topTravel(){

    val totalTravel = utils.readData(pathOD)
      .groupBy("start_station_code","end_station_code").count()
      .withColumnRenamed("count","total")
      .orderBy(desc("total")).limit(10)

    totalTravel.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .mode("overwrite")
      .save("src/results/topTravel" + year)

  }

  def topRushHour(){

    val ODByHour = utils.readData(pathOD)
        .select($"start_date",unix_timestamp($"start_date", "yyyy-MM-dd HH:mm").cast(TimestampType).as("timestamp"))
        .withColumn("hour", hour($"timestamp"))
        .groupBy("hour").count()
        .withColumnRenamed("count", "total")
        .orderBy(desc("total")).limit(5)

    ODByHour.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .mode("overwrite")
      .save("src/results/topRushHours" + year)

  }


  def averageSpeed(){

    val odData = utils.readData(pathOD).withColumnRenamed("_c0","id_travel")
    val stations = utils.readData(pathStations)

    val originStation = odData.withColumnRenamed("start_station_code","code")
      .join(stations,"code")
      .withColumnRenamed("latitude","lat1")
      .withColumnRenamed("longitude","lon1")
      .select("id_travel","lat1","lon1")

    val destinationStation = odData.withColumnRenamed("end_station_code","code")
      .join(stations,"code")
      .withColumnRenamed("latitude","lat2")
      .withColumnRenamed("longitude","lon2")
      .select("id_travel","lat2","lon2")

    val distanceStations = originStation.join(destinationStation, "id_travel")
      .withColumn("lat1",($"lat1"*Math.PI)/180)
      .withColumn("lat2",($"lat2"*Math.PI)/180)
      .withColumn("lon1",($"lon1"*Math.PI)/180)
      .withColumn("lon2",($"lon2"*Math.PI)/180)
      .withColumn("a", pow(sin(($"lat2" - $"lat1") / 2), 2) +
        cos(($"lat1")) * cos(($"lat2")) * pow(sin(($"lon2" - $"lon1") / 2), 2))
      .withColumn("distance_km", atan2(sqrt($"a"), sqrt(-$"a" + 1)) * 2 * 6371)
        .orderBy("id_travel")

    val totalDistance = distanceStations.select($"distance_km").rdd.map(_(0).asInstanceOf[Double]).reduce(_+_)

    val totalHours = odData.withColumn("duration_hour",$"duration_sec" / 3600)
      .select($"duration_hour").rdd.map(_(0).asInstanceOf[Double]).reduce(_+_)

    val speed = totalDistance/totalHours

    println("The average speed is\n" +
      speed + " Km/h")

  }
}