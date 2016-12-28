package gg.boosted.services

import java.time.{LocalDateTime, Period, ZoneId}
import java.util.Date

import gg.boosted.analyzers.BoostedSummonersAnalyzer
import gg.boosted.configuration.Configuration
import gg.boosted.posos.SummonerMatch
import gg.boosted.{Application, Role}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{LabeledPoint, OneHotEncoder, StringIndexer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.streaming.dstream.DStream
import org.slf4j.LoggerFactory

/**
  * Created by ilan on 8/26/16.
  */
object AnalyzerService {

    val log = LoggerFactory.getLogger(AnalyzerService.getClass)

    val maxRank = Configuration.getInt("maxrank")

    val minGamesPlayed = Configuration.getInt("min.games.played")

    def analyze(stream:DStream[SummonerMatch]):Unit = {

        //At this point i'm not sure why i need to work with DStreams at all so:
        stream.foreachRDD(rdd => {
            log.info("Processing at: " + new Date()) ;
            analyze(rdd)
        })
    }

    def analyze(rdd:RDD[SummonerMatch]):Unit = {
        //Convert the rdd to ds so we can use Spark SQL on it
        if (rdd != null) {
            analyze(convertToDataSet(rdd))
        } else {
            log.debug("RDD is empty")
        }

    }

    def analyze(ds:Dataset[SummonerMatch]):Unit = {
        //Get the boosted summoner DF by champion and role
        log.debug(s"Retrieved ${ds.count()} rows")

        //BoostedSummonersAnalyzer.process(ds, minGamesPlayed, getDateToLookForwardFrom, maxRank)

        val bs = BoostedSummonersAnalyzer.findBoostedSummoners(ds, 3, 0, 1000)

        val me = BoostedSummonersAnalyzer.boostedSummonersToMatchSummary(bs)

        import Application.session.implicits._
        val labeled = me.map(r => LabeledPoint(if (r._1) 1.0 else 0.0, Vectors.dense(r._7)))

        val lrModel = new LogisticRegression().setMaxIter(10).fit(labeled)

        println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
        me.show()


//        val me = BoostedSummonersAnalyzer.boostedSummonersToMatchSummary(bs, null)
//
//        var transforming = new StringIndexer().setInputCol("itemId").setOutputCol("itemIdx").fit(me).transform(me)
//
//        transforming = new OneHotEncoder().setInputCol("itemIdx").setOutputCol("itemVec").transform(transforming)
//
//        transforming.show()


        //I need to encode itemIds and roles
//        me.map(r => {
//              val winnerInt = if (r.winner) 1 else 0
//              (r.championId,
//                Role.valueOf(r.role).roleId,
//                r.timeStamp,
//                r.itemId,
//                winnerInt)
//          }).map(r => LabeledPoint(r._5, Vectors.dense(r._1, r._2, r._3, r._4)))
//
//        me.show(10000, false)

        //val lrModel = new LogisticRegression().setMaxIter(10).fit(me)

        //println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")





//        val roleIndexer = new StringIndexer()
//          .setInputCol("role")
//          .setOutputCol("roleIndex")
//          .fit(me)
//
//        val indexed = roleIndexer.transform(me)
//
//        val transformed = new VectorAssembler()
//          .setInputCols(Array("championId", "roleIndex", "timestamp", "itemId"))
//          .setOutputCol("features").transform(indexed)
//
//        val labeled = transformed.selectExpr("cast (winner as int) winner", "features")
//
//        labeled.show()
//
//        val lr = new LogisticRegression()
//          .setMaxIter(10).setLabelCol("winner").setFeaturesCol("features")
//
//        val lrModel = lr.fit(labeled)






        //log.info(s"Retrieved total of ${topSummoners.length} boosted summoners")
    }

    def convertToDataSet(rdd:RDD[SummonerMatch]):Dataset[SummonerMatch] = {
        import Application.session.implicits._
        return Application.session.createDataset(rdd)
    }

    def getDateToLookForwardFrom():Long = {
        //Get the date to start looking from
        val backPeriodInMinutes = Configuration.getLong("window.size.minutes")
        val backPeriodInDays = (backPeriodInMinutes / 60  /24).toInt
        val backPeriod = (LocalDateTime.now().minus(Period.ofDays(backPeriodInDays)))
        val zoneId = ZoneId.of("UTC")
        return backPeriod.atZone(zoneId).toEpochSecond() * 1000
    }

}
