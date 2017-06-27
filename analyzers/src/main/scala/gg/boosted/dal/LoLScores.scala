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
        ds.rdd.foreachPartition(partitionOfRecords => {

            //Run over all the records and get the summonerId field of each one
            var allSummonerIds = Set[SummonerId]()

            partitionOfRecords.foreach(row => allSummonerIds += SummonerId(row.summonerId, Platform.valueOf(row.region)))

            var unknownScores = Set[SummonerId]()

            allSummonerIds.foreach(id =>
                RedisStore.getSummonerLOLScore(id).getOrElse(unknownScores += id)
            )

            unknownScores.groupBy(_.platform).par.foreach(tuple => {
                val platform = tuple._1
                val ids = tuple._2
                val api = new RiotApi(platform)

                ids.map(_.id).foreach { id =>
                  val position = api.getLeaguePosition(id)
                  val lolScore = LoLScore(position.tier, position.rank, position.leaguePoints)
                }
            })
        })
    }

    def get(summonerId: Long, region: String): LoLScore = {
        get(SummonerId(summonerId, Platform.valueOf(region)))
    }

    def get(summonerId: SummonerId): LoLScore = {
        RedisStore.getSummonerLOLScore(summonerId) match {
            case Some(x) => x
            case None => throw new RuntimeException(s"The lol score for ${summonerId.id} at ${summonerId.platform.toString} is unaccounted for. did you populateAndBroadcast first?")
        }
    }

}
