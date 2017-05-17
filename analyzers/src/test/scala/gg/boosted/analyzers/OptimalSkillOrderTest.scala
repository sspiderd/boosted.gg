package gg.boosted.analyzers

import java.util.Date

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec}

/**
  * Created by ilan on 2/13/17.
  */
class OptimalSkillOrderTest extends FlatSpec with BeforeAndAfter{

  private val master = "local[*]"
  private val appName = "OptimalSkillOrderTest"

  private val now = new Date().getTime ;

  private val spark:SparkSession = SparkSession
    .builder()
    .appName(appName)
    .master(master)
    .getOrCreate()

  import spark.implicits._

  "A test with no name" should "be returned" in {

  }

}
