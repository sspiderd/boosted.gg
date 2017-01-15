package gg.boosted.dal

import gg.boosted.posos.{BoostedSummoner, SummonerId}
import gg.boosted.riotapi.{Region, RiotApi}
import org.apache.spark.sql.Dataset

import scala.collection.JavaConverters._

/**
  * Created by ilan on 1/15/17.
  */
object SummonerNames {


    def populateNames(ds: Dataset[BoostedSummoner]): Unit = {
        ds.foreachPartition(partitionOfRecords => {

            //Run over all the records and get the summonerId field of each one
            var allSummonerIds = Set[SummonerId]()
            partitionOfRecords.foreach(row => allSummonerIds += SummonerId(row.summonerId, Region.valueOf(row.region)))

            //Find names and scores that we don't know yet
            var unknownNames = Set[SummonerId]()

            allSummonerIds.foreach(id =>
                RedisStore.getSummonerName(id).getOrElse(unknownNames += id)
            )

            //group by regions and call their apis
            unknownNames.groupBy(_.region).par.foreach(tuple => {
                val region = tuple._1
                val ids = tuple._2
                val api = new RiotApi(region)
                api.getSummonerNamesByIds(ids.map(_.id).toList.map(Long.box):_*).asScala.foreach(
                    mapping => RedisStore.addSummonerName(SummonerId(mapping._1, region), mapping._2))
            })
        })
    }

    def get(summonerId:Long, region:String):String = {
        get(SummonerId(summonerId, Region.valueOf(region)))
    }

    def get(summonerId: SummonerId):String = {
        RedisStore.getSummonerName(summonerId) match {
            case Some(x) => x
            case None => throw new RuntimeException(s"The name for ${summonerId.id} at ${summonerId.region.toString} is unaccounted for. did you populateAndBroadcast first?")
        }
    }

}
