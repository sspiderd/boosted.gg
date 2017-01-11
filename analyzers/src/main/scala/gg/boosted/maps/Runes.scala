package gg.boosted.maps

import gg.boosted.riotapi.{Region, RiotApi}
import gg.boosted.riotapi.dtos.Item
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by ilan on 1/11/17.
  */
object Runes {
    val log:Logger = LoggerFactory.getLogger(Items.getClass) ;

    var runes = collection.mutable.HashMap.empty[Int, Item]

    val riotApi = new RiotApi(Region.EUW)

    def populateMap(): Unit = {
        import collection.JavaConverters._

        riotApi.getItems.asScala.foreach(item => runes(item._1) = item._2)
    }

    def populateMapIfEmpty(): Unit = {
        if (runes.size == 0) {
            populateMap()
        }
    }

    def byId(id:Int):Item = {
        runes.get(id) match {
            case Some(item) => item
            case None =>
                log.debug("Item id {} not found. Downloading from riot..", id)
                //We can't find the id, load it from riot
                populateMap()
                runes.get(id).get
        }
    }
}
