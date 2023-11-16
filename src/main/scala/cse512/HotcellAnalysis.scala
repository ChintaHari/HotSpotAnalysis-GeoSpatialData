package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)
  
  pickupInfo.createOrReplaceTempView("pickupInfo")  

  //Calculating Xj attribute value 
  
  spark.udf.register("checkIfTheCellIsInRange", (x: Double, y:Double, z:Int) =>  ( (x >= minX) && (x <= maxX) && (y >= minY) && (y <= maxY) && (z >= minZ) && (z <= maxZ) ))
    
  val cellPoints = spark.sql("select x,y,z,count(*) as numberOfPoints from pickupInfo where checkIfTheCellIsInRange(x,y,z) group by x,y,z").persist()
  cellPoints.createOrReplaceTempView("cellPoints")   
  
    
  val p = spark.sql("select sum(numberOfPoints) as xj, sum(numberOfPoints*numberOfPoints) as SquareOfXj from cellPoints").persist()
  //val xj = p.first().getLong(0).toDouble
  //val SquareOfXj = p.first().getLong(1).toDouble  
  
  val mean = ((p.first().getLong(0).toDouble)/numCells)
    
    
  val standard_deviation = Math.sqrt(((p.first().getLong(1).toDouble)/numCells) - (mean*mean))   
  
  val neighbouringView = spark.sql("select gp1.x as x , gp1.y as y, gp1.z as z, count(*) as numberOfNeighbours, sum(gp2.numberOfPoints) as sigma from cellPoints as gp1 inner join cellPoints as gp2 on ((abs(gp1.x-gp2.x) <= 1 and  abs(gp1.y-gp2.y) <= 1 and abs(gp1.z-gp2.z) <= 1)) group by gp1.x, gp1.y, gp1.z").persist()
    
  neighbouringView.createOrReplaceTempView("neighbouringView")
  
  spark.udf.register("getZScore",(mean: Double, standardDeviation:Double, numberOfNeighbours: Int, sigma: Int, numCells:Int)=>((
    HotcellUtils.getZScore(mean, standardDeviation, numberOfNeighbours, sigma, numCells)
    )))  
  
  val zScoreView =  spark.sql("select x,y,z,getZScore("+ mean + ","+ standard_deviation +",numberOfNeighbours,sigma," + numCells+") as zscore from neighbouringView")
  zScoreView.createOrReplaceTempView("zScoreView")
  
  val result = spark.sql("select x,y,z from zScoreView order by zscore desc")
  return result
  
}
}
