package gg.boosted.maps

import gg.boosted.Application
import gg.boosted.riotapi.dtos.RuneDef
import gg.boosted.riotapi.{Platform, RiotApi}
import org.apache.spark.broadcast.Broadcast
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by ilan on 1/11/17.
  */
object Runes {
    val log:Logger = LoggerFactory.getLogger(Items.getClass) ;

    var runesBr:Broadcast[Map[String,RuneDef]] = _

    val riotApi = new RiotApi(Platform.EUW)

    def populateAndBroadcast():Unit = {
        import collection.JavaConverters._
        val runes = riotApi.getRuneDefs.asScala.map {case (k,v) => (k, v)}.toMap
        runesBr = Application.session.sparkContext.broadcast(runes)
    }

    def runes():Map[String,RuneDef] = {
        runesBr.value
    }

    def byId(id:String):RuneDef = {
        runes().get(id) match {
            case Some(rune) => rune
            case None =>
                log.debug("Item id {} not found. Downloading from riot..", id)
                //We can't find the id, load it from riot
                populateAndBroadcast()
                runes.get(id).get
        }
    }
}
