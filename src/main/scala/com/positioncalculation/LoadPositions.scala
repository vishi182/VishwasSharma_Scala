package com.positioncalculation

import org.apache.spark.sql.DataFrame

object LoadPositions extends PositionCalcInit {

  /**
    *
    * @param Instrument
    * @param Account
    * @param AccountType
    * @param Quantity
    */
  case class SODPositions(Instrument: String,
                          Account: String,
                          AccountType: String,
                          Quantity: Int
                         )

  /**
    *
    * @param fileName
    * @return
    */

  def readSODPositions(fileName: String): DataFrame = {
    import spark.implicits._
    val read_file = spark.read.option( "header", "true" ).option("inferSchema","true").csv( RESOURCE_ROOT + fileName )
    val df = read_file.as[SODPositions].toDF()
    df
  }

}