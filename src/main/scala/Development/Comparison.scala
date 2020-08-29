package Development

import Commons.Utils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

class Comparison (utils: Utils, year1: String, year2:String) {

  import utils.spark.implicits._

  val pathOD1 = "src/data/OD/OD_" + year1 + ".csv"
  val pathStations1 = "src/data/Stations/Stations_" + year1 + ".csv"

  val pathOD2 = "src/data/OD/OD_" + year2 + ".csv"
  val pathStations2 = "src/data/Stations/Stations_" + year2 + ".csv"


  def totalTravel(){

    val totalTravelYear1 = utils.readData(pathOD1).count()
    val totalTravelYear2 = utils.readData(pathOD2).count()
    val totalTravelDif = totalTravelYear2 - totalTravelYear1

    val totalTravelDifDf = Seq ((year1,totalTravelYear1),(year2,totalTravelYear2), ("diff",totalTravelDif))
      .toDF("Reference","Value")

    println("Comparison of the difference in the number of travel between the year " + year1 + " and " + year2)
    totalTravelDifDf.show()
  }

  def totalTimeUse(){

    val totalTimeUseYear1 = utils.readData(pathOD1).withColumn("duration_hour",$"duration_sec" / 3600)
      .select($"duration_hour").rdd.map(_(0).asInstanceOf[Double]).reduce(_+_)

    val totalTimeUseYear2 = utils.readData(pathOD2).withColumn("duration_hour",$"duration_sec" / 3600)
      .select($"duration_hour").rdd.map(_(0).asInstanceOf[Double]).reduce(_+_)

    val totalTimeUseDif = totalTimeUseYear2 - totalTimeUseYear1

    val totalTimeUseDifDf = Seq ((year1,totalTimeUseYear1),(year2,totalTimeUseYear2), ("diff",totalTimeUseDif))
      .toDF("Reference","Value(Hours)")

    println("Comparison of the difference in the number of hours of use between the year " + year1 + " y " + year2)
    totalTimeUseDifDf.show()

  }

  /**
   * Function that creates a help table to compare values
   * @return Dataframe
   */
  def totalCapacity(): DataFrame = {

    val StationsYear1 = utils.readData(pathStations1)
    val StationsYear2 = utils.readData(pathStations2)

    val path = "src/data/stations.json"
    val capacityStationsDF = utils.spark.read
        .format("json")
        .option("header","true")
        .load(path)
        .select(explode($"stations").alias("data"))
        .withColumn("code",$"data".getItem("n"))
        .withColumn("da",$"data".getItem("da"))
        //.withColumn("lc",($"data".getItem("lc")/1000).cast(DataTypes.TimestampType))

    val totalTravelsYear1 = utils.readData(pathOD1).count()
    val totalTravelsYear2 = utils.readData(pathOD2).count()

    val totalStationsYear1 = utils.readData(pathStations1).count()
    val totalStationsYear2 = utils.readData(pathStations2).count()

    val totalCapacityYear1 = StationsYear1.join(capacityStationsDF, "code")
      .select($"da").rdd.map(_(0).asInstanceOf[Long]).reduce(_+_)

    val totalCapacityYear2 = StationsYear2.join(capacityStationsDF, "code")
      .select($"da").rdd.map(_(0).asInstanceOf[Long]).reduce(_+_)

    val capacityComp = Seq ((year1,totalTravelsYear1,totalStationsYear1,totalCapacityYear1),
      (year2,totalTravelsYear2,totalStationsYear2,totalCapacityYear2))
      .toDF("year","total_travels","total_stations","bikes_capacity")

    capacityComp
  }

  def travelsByStationsBikes(){

    val travelsByStationsBikesdf = totalCapacity().withColumn("travel_per_stations", $"total_travels"/$"total_stations")
      .withColumn("travel_per_bikes", $"total_travels"/$"bikes_capacity")

    println("Comparison travel per stations and travel per bikes between the year " + year1 + " y " + year2)
    travelsByStationsBikesdf.show()
  }

  def comparisonCapacity(){

    val diffCapacity = totalCapacity().take(2)(1)(3).asInstanceOf[Long] - totalCapacity().take(1)(0)(3).asInstanceOf[Long]

    println("The difference in capacity between the year " + year1 + " y " + year2 +" is: \n" +
      diffCapacity)
  }

  def capacityInstall() {

    val path = "src/data/stations.json"
    val capacityPerStation = utils.spark.read
      .format("json")
      .option("header", "true")
      .load(path)
      .select(explode($"stations").alias("data"))
      .withColumn("da", $"data".getItem("da"))

    val totalCapacityInstall = capacityPerStation
      .select($"da").rdd.map(_(0).asInstanceOf[Long]).reduce(_+_)

    println("The total capacity of Bixi is: " + totalCapacityInstall)
  }

}
