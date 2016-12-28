package gg.boosted.maps

import gg.boosted.riotapi.dtos.Item
import gg.boosted.riotapi.{Region, RiotApi}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by ilan on 12/27/16.
  */
object Items {
    val log:Logger = LoggerFactory.getLogger(Items.getClass) ;

    var items = collection.mutable.HashMap.empty[Int, Item]

    var legendaryCutoff = 1200

    val riotApi = new RiotApi(Region.EUW)

    def populateMap(): Unit = {
        import collection.JavaConverters._

        riotApi.getItems.asScala.foreach(item => items(item._1) = item._2)
    }

    def populateMapIfEmpty(): Unit = {
        if (items.size == 0) {
            populateMap()
        }
    }

    def byId(id:Int):Item = {
        items.get(id) match {
            case Some(item) => item
            case None =>
                log.debug("Item id {} not found. Downloading from riot..", id)
                //We can't find the id, load it from riot
                populateMap()
                items.get(id).get
        }
    }

    def legendary():Map[Int, Item] = {
        items.filter(_._2.gold >= legendaryCutoff).toMap
    }


    def main(args: Array[String]): Unit = {
        print (Items.byId(2010).name)
    }
}