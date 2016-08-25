package gg.boosted.analyzers

import java.util.Date

import gg.boosted.{Role, SummonerMatch, Tier}
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec}
import org.apache.spark.sql.functions._


case class SM(matchId:Long, summonerId:Long, championId:Int, role:String, winner:Boolean, region: String, date: Long, tier: String)

/**
  * Created by ilan on 8/25/16.
  */
class HighestCHrolesWRTest extends FlatSpec with BeforeAndAfter{

    private val master = "local[*]"
    private val appName = "MostBoostedSummonersPlayingChampionAtRoleTests"

    private val now = new Date().getTime ;

    private var spark:SparkSession = _
    

    before {
        spark = SparkSession
                .builder()
                .appName(appName)
                .master(master)
                .getOrCreate()



    }

    after {
    }

    "A test" should "be tested" in {
        //val langPercentDF = spark.createDataFrame(List(("Scala", 35), ("Python", 30), ("R", 15), ("Java", 20)))

        //val lpDF = langPercentDF.withColumnRenamed("_1", "language").withColumnRenamed("_2", "percent")
        //lpDF.orderBy(desc("percent")).show()
        //order the DataFrame in descending order of percentage
        //lpDF.orderBy(desc("percent")).show(false)

//        val smFrame = spark.createDataFrame(List(
//            SM(1,1,1,"TOP", true, "NA", now, "GOLD"),
//            SM(1,2,2,"MIDDLE", true, "NA", now, "GOLD"),
//            SM(1,3,3,"JUNGLE", false, "NA", now, "GOLD"),
//            SM(2,1,1,"TOP", true, "NA", now, "GOLD"),
//            SM(2,2,2,"MIDDLE", false, "NA", now, "GOLD"),
//            SM(2,3,3,"JUNGLE", false, "NA", now, "GOLD")
//        ))
        //smFrame.select("championId", "role").show()
        //smFrame.select("championId", "role", ).show()

        //smFrame.createOrReplaceTempView("SM")
        //spark.sql("SELECT championId, role, sum(if (winner==true, 1,  0))/count(winner) as winrate FROM SM group by championId, role").show()
        //smFrame.show()


        val spark:SparkSession


        val smSet = spark.createDataset[SM](List(
                    SM(1,1,1,"TOP", true, "NA", now, "GOLD"),
                    SM(1,2,2,"MIDDLE", true, "NA", now, "GOLD"),
                    SM(1,3,3,"JUNGLE", false, "NA", now, "GOLD"),
                    SM(2,1,1,"TOP", true, "NA", now, "GOLD"),
                    SM(2,2,2,"MIDDLE", false, "NA", now, "GOLD"),
                    SM(2,3,3,"JUNGLE", false, "NA", now, "GOLD")
        ))

        smSet.createOrReplaceTempView("SM")
        smSet.sparkSession.sql("SELECT championId, role, sum(if (winner==true, 1,  0))/count(winner) as winrate FROM SM group by championId, role").show()
    }
}
