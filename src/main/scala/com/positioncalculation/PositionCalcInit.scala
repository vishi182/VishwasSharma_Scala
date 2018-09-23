package com.positioncalculation

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

trait PositionCalcInit {

  private val master = "local[1]"
  private val appName = "LOTR Weather"

  val spark: SparkSession = SparkSession.builder().appName(appName).master(master).getOrCreate()

  final val RESOURCE_ROOT = "src/main/resources/"

  //File stores start positions for instruments
  final val I_SOD_POS = "Input_StartOfDay_Positions.txt"

  //File contains transactions for a day
  final val I_TRAN = "Input_Transactions.txt"

  //Expected output file
  final val E_EOD_POS = "EonOfDay_Positions.txt"

}
