package com

import Development.yearAnalysis
import Commons.Utils
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class test extends FunSuite {

  sys.props("testing") = "true"

  SparkSession.builder().master("local").appName("spark session").getOrCreate()

  val ut = new Utils
  val spark: SparkSession = ut.spark

  val yearAnalysisTests = new yearAnalysis(ut,"test")

  test("ObligatoriosTests.pathOD should be src/data/OD/OD_test.csv") {
    assert(yearAnalysisTests.pathOD === "src/data/OD/OD_test.csv")
  }

  test("ObligatoriosTests.pathStations should be src/data/Stations/Stations_test.csv") {
    assert(yearAnalysisTests.pathStations === "src/data/Stations/Stations_test.csv")
  }

  yearAnalysisTests.histogram()
  val histogramTest = ut.readData("src/results/ODByMonthtest")
    test("Histogram result should [04,5.0], [05,14.0] ") {
    assert(histogramTest.take(2)(0)(1) === 5.0)
    assert(histogramTest.take(2)(1)(1) === 14.0)
  }

  yearAnalysisTests.topStations()
  val totalEndStationsTest = ut.readData("src/results/totalEndStationstest")
  val totalStartStationsTest = ut.readData("src/results/totalStartStationstest")
  val totalInGeneralTest = ut.readData("src/results/totalInGeneraltest")
  test("TopsStations Analysis\n" +
    "The size of the tables should be 3\n" +
    "The top Start Station should be Chapleau / du Mont-Royal with 7 times used\n" +
    "The top End Station should be Square St-Louis with 5 times used\n" +
    "The top Start/End Station should be Chapleau / du Mont-Royal with 9 times used") {
    assert(totalEndStationsTest.count() === 3)
    assert(totalStartStationsTest.count() === 3)
    assert(totalInGeneralTest.count() === 3)
    assert(totalStartStationsTest.take(1)(0)(1) === 7)
    assert(totalStartStationsTest.take(1)(0)(2) === "Chapleau / du Mont-Royal")
    assert(totalEndStationsTest.take(1)(0)(1) === 5)
    assert(totalEndStationsTest.take(1)(0)(2) === "Mansfield / Sherbrooke")
    assert(totalInGeneralTest.take(1)(0)(4) === 10)
    assert(totalInGeneralTest.take(1)(0)(1) === "Chapleau / du Mont-Royal")
  }

  yearAnalysisTests.topTravel()
  val topTravelTest = ut.readData("src/results/topTraveltest")
  test("Top travel should be between 6149 and 6068 and 4 times in total") {
    assert(topTravelTest.take(1)(0)(0) === 6149)
    assert(topTravelTest.take(1)(0)(1) === 6068)
    assert(topTravelTest.take(1)(0)(2) === 4)
  }

  yearAnalysisTests.topRushHour()
  val topRushHourTest = ut.readData("src/results/topRushHourstest")
  test("TopRushHour should be at 18") {
    assert(topRushHourTest.take(1)(0)(0) === 18)
  }

}
