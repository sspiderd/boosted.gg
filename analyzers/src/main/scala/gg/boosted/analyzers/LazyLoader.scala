package gg.boosted.analyzers

import gg.boosted.dal.RedisStore
import gg.boosted.maps.RiotApis
import gg.boosted.posos.SummonerId
import gg.boosted.riotapi.Region

import scala.collection.mutable.ListBuffer

/**
  * Created by ilan on 12/16/16.
  */
object LazyLoader {

    val nameMap = scala.collection.mutable.HashMap.empty[SummonerId, String]

    val unknownNames = new ListBuffer[SummonerId]

    def loadNameLazy(summonerId:Long, region:String):Boolean = {
        val id = SummonerId(summonerId, Region.valueOf(region))
        nameMap.getOrElse(id, {
            //If it's already in the map, we're good
            //If not...
            nameMap.synchronized {
                //Search for it in the redis store...
                RedisStore.getSummonerName(id) match {
                    case Some(name) => nameMap(id) = name //We know this name, put it in the map
                    case None => unknownNames += id //We've never heard of that dude (well, for the past TTL days, anyway
                }
            }
        })

        //The return type doesn't really matter. it just needs to be there
        return true

    }

    def getName(summonerId:Long, region:String):String = {
        val id = SummonerId(summonerId, Region.valueOf(region))
        nameMap.getOrElse(id, {
            //The name is not in the map, we need to call the api
            //So we call the api for all of our unknowns
            //We need to call the api just once, otherwise it's a waste so we get a lick
            unknownNames.synchronized {
                nameMap.getOrElse(id, {
                    import collection.JavaConverters._
                    val summonersForRegion = unknownNames.filter(_.region != id.region).map(_.id).map(Long.box)
                    val summonerMap = RiotApis.get(id.region).getSummonerNamesByIds(summonersForRegion:_*).asScala
                    //Make a map of SummonerIds
                    summonerMap.map(entry => (SummonerId(entry._1, Region.valueOf(region)), entry._2)).foreach(entry => nameMap(entry._1) = entry._2)
                    nameMap.get(id).get
                })
            }
        })
    }



}
