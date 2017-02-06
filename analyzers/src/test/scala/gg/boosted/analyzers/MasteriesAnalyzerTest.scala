package gg.boosted.analyzers

import java.util.Date

import gg.boosted.riotapi.dtos.`match`.Mastery
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec}

/**
  * Created by ilan on 2/6/17.
  */
class MasteriesAnalyzerTest extends FlatSpec with BeforeAndAfter{

  private val master = "local[*]"
  private val appName = "MasteriesAnalyzerTest"

  private val spark:SparkSession = SparkSession
    .builder()
    .appName(appName)
    .master(master)
    .getOrCreate()

  import spark.implicits._

  "Sending masteries" should "return themselves" in {
    val mastery1 = Seq[Mastery]
  }

}
