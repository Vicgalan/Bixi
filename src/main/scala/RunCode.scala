import Commons.Utils
import Development.{Comparison, yearAnalysis}
import org.apache.log4j.{Level, Logger}

object RunCode extends {

  val utils = new Utils

  def main(args: Array[String]): Unit = {

    args match {

      case Array("-H",_) =>
        new yearAnalysis(utils,args(1)).histogram()

      case Array("-TS",_) =>
        new yearAnalysis(utils,args(1)).topStations()

      case Array("-TT",_) =>
        new yearAnalysis(utils,args(1)).topTravel()

      case Array("-TBH",_) =>
        new yearAnalysis(utils,args(1)).topRushHour()

      case Array("-TopALL",_) =>
        new yearAnalysis(utils,args(1)).histogram()
        new yearAnalysis(utils,args(1)).topStations()
        new yearAnalysis(utils,args(1)).topTravel()
        new yearAnalysis(utils,args(1)).topRushHour()

      case Array("-CTravel",_,_) =>
        new Comparison(utils,args(1),args(2)).totalTravel()

      case Array("-CTime",_,_) =>
        new Comparison(utils,args(1),args(2)).totalTimeUse()

      case Array("-CTbSB",_,_) =>
        new Comparison(utils,args(1),args(2)).travelsByStationsBikes()

      case Array("-CapInstall") =>
        new Comparison(utils,"x","x").capacityInstall()

      case Array("-CCapacity",_,_) =>
        new Comparison(utils,args(1),args(2)).comparisonCapacity()

      case Array("-Speed",_) =>
        new yearAnalysis(utils,args(1)).averageSpeed()

      case _ =>
        println("ERROR: Invalid option")
        throw new IllegalArgumentException("Wrong arguments. Usage: \n" +
          "Check the README.md to choose a correct option ")
    }

  }

}
