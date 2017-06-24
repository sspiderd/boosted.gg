package gg.boosted.dal

import gg.boosted.posos.{BoostedSummoner, LoLScore, SummonerId}
import gg.boosted.riotapi.{Platform, RiotApi}
import org.apache.spark.sql.Dataset

import scala.collection.JavaConverters._

/**
  * Created by ilan on 1/15/17.
  */
object LoLScores {

    def populateScores(ds: Dataset[BoostedSummoner]): Unit = {
        ds.foreachPartition(partitionOfRecords => {

            //Run over all the records and get the summonerId field of each one
            var allSummonerIds = Set[SummonerId]()
            partitionOfRecords.foreach(row => allSummonerIds += SummonerId(row.summonerId, Platform.valueOf(row.region)))

            var unknownScores = Set[SummonerId]()

            allSummonerIds.foreach(id =>
                RedisStore.getSummonerLOLScore(id).getOrElse(unknownScores += id)
            )

            unknownScores.groupBy(_.region).par.foreach(tuple => {
                val region = tuple._1
                val ids = tuple._2
                val api = new RiotApi(region)

                api.getLeagueEntries(ids.map(_.id).toList.map(Long.box): _*).asScala.foreach(
                    mapping => {
                        val lolScore = LoLScore(mapping._2.tier, mapping._2.division, mapping._2.leaguePoints)
                        RedisStore.addSummonerLOLScore(SummonerId(mapping._1, region), lolScore)
                    })
            })
        })
    }

    def get(summonerId: Long, region: String): LoLScore = {
        get(SummonerId(summonerId, Platform.valueOf(region)))
    }

    def get(summonerId: SummonerId): LoLScore = {
        RedisStore.getSummonerLOLScore(summonerId) match {
            case Some(x) => x
            case None => throw new RuntimeException(s"The lol score for ${summonerId.id} at ${summonerId.region.toString} is unaccounted for. did you populateAndBroadcast first?")
        }
    }

}
