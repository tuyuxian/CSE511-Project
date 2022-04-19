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

  // YOU NEED TO CHANGE THIS PART
  // create a temporary view
  pickupInfo.createOrReplaceTempView("pickupInfo")
  // select points in the given zone and count x_j for every selected points 
  val pointInZone = spark.sql(s"select x, y, z, count(*) as xj from pickupInfo where x>=$minX and x<=$maxX and y>=$minY and y<=$maxY and z>=$minZ and z<=$maxZ group by x, y, z").persist()
  // create a temporary view
  pointInZone.createOrReplaceTempView("pointInZone")
  // select sigma of x and sigma of x to the power of 2
  val temp = spark.sql("select sum(xj) as sumOfX, sum(xj*xj) as sumOfXSquare from pointInZone").persist()
  // calculate x_bar and sd for parameter
  val xBar = (temp.first().getLong(0).toDouble/numCells)
  val sd = math.sqrt(temp.first().getLong(1).toDouble/numCells - (xBar*xBar))
  // calculate the sumOfW and sumOfWX for every distinct point
  val pointInNeighbor = spark.sql( "select t1.x, t1.y, t1.z, count(t2.xj) as sumOfW, sum(t2.xj) as sumOfWX from pointInZone as t1, pointInZone as t2 where (abs(t1.x - t2.x)<=1 and abs(t1.y - t2.y)<=1 and abs(t1.z - t2.z)<=1) group by t1.x, t1.y, t1.z").persist()
  // create a temporary view
  pointInNeighbor.createOrReplaceTempView("pointInNeighbor")
  // create udf for gScore
  spark.udf.register("gScore",(sumOfWX: Int, xBar: Double, sumOfW: Int, sd: Double, N: Int) => ((
    HotcellUtils.gScore(sumOfWX, xBar, sumOfW, sd, N)
    )))
  // calculate gScore
  val calculateGScore = spark.sql(s"select x, y, z, gScore(sumOfWX, $xBar, sumOfW, $sd, $numCells) as gScore from pointInNeighbor").persist()
  // create a temporary view
  calculateGScore.createOrReplaceTempView("calculateGScore")
  // get the final result
  val result = spark.sql("select x,y,z from calculateGScore order by gScore desc limit 50").persist()
  // return the result
  return result
}
}
