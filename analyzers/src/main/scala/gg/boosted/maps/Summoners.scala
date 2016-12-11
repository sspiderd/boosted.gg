package gg.boosted.maps

import gg.boosted.{Region, RiotApi}
import gg.boosted.dal.RedisStore
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

/**
  *
  * Returns stuff about summoners
  *
  * Created by ilan on 12/8/16.
  */
object Summoners {

    val log = LoggerFactory.getLogger(Summoners.getClass)

    /**
      * Returns a map of id->name for the given ids
      * @param ids
      * @return
      */
    def getNames(ids:Seq[Long], region:String):Map[Long, String] = {
        val map = scala.collection.mutable.HashMap.empty[Long, String]

        //We search redis for the id. if it is there, we simply put it in the map, if it isn't we add to the list of
        //the unknowns
        val unknowns = new ListBuffer[Long]

        ids.foreach(id => {
            RedisStore.getSummonerNameById(region, id) match {
                case Some(name) => map(id) = name //We know this name, put it in the map
                case None => unknowns += id //We've never heard of that dude (well, for the past TTL days, anyway
            }
        })

        log.debug("Found {} summoner names already in store", map.size)

        val riotApi = new RiotApi(Region.valueOf(region))

        import collection.JavaConverters._
        riotApi.getSummonerNamesByIds(unknowns.map(Long.box):_*).asScala.foreach(mapping => {
            val id = mapping._1
            val name = mapping._2
            log.debug("Adding new summoner to redis store {} -> {}", id, name, "")
            //Insert each of the new names to the store
            RedisStore.addSummonerName(region, id, name)
            //And to the map
            map(id) = name
        })

        return map.toMap
    }



}
