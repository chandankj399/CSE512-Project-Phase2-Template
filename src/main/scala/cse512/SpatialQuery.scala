package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>({
      val rectanglePoints = queryRectangle.split(",")
      val x1 = rectanglePoints(0).toFloat
      val y1 = rectanglePoints(1).toFloat
      val x2 = rectanglePoints(2).toFloat
      val y2 = rectanglePoints(3).toFloat
      val points = pointString.split(",")
      val x = points(0).toFloat
      val y = points(1).toFloat
      (x >= x1 && x <= x2) && (y >= y1 && y <= y2)
    })
    )

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()
    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>({
      val rectanglePoints = queryRectangle.split(",")
      val x1 = rectanglePoints(0).toFloat
      val y1 = rectanglePoints(1).toFloat
      val x2 = rectanglePoints(2).toFloat
      val y2 = rectanglePoints(3).toFloat
      val points = pointString.split(",")
      val x = points(0).toFloat
      val y = points(1).toFloat
      (x >= x1 && x <= x2) && (y >= y1 && y <= y2)
    }
    )
    )

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()
    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")
println("arg1: ",arg1)
    println("arg2: ",arg2)
    println("arg3: ",arg3)
    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>({
      val point1 = pointString1.split(",")
      val point2 = pointString2.split(",")
      val point1X = point1(0).toFloat
      val point1Y = point1(1).toFloat
      val point2X = point2(0).toFloat
      val point2Y = point2(1).toFloat

      Math.sqrt((point2X - point1X) * (point2X - point1X) + (point2Y - point1Y) * (point2Y - point1Y)) <= distance.toFloat

    }))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()
    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>({
      val point1 = pointString1.split(",")
      val point2 = pointString2.split(",")
      val point1X = point1(0).toFloat
      val point1Y = point1(1).toFloat
      val point2X = point2(0).toFloat
      val point2Y = point2(1).toFloat

      Math.sqrt((point2X - point1X) * (point2X - point1X) + (point2Y - point1Y) * (point2Y - point1Y)) <= distance.toFloat
    }))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()
    return resultDf.count()
  }
}
