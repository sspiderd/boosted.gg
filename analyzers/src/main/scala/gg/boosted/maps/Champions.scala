package gg.boosted.maps

import gg.boosted.riotapi.{Region, RiotApi}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by ilan on 12/9/16.
  */
object Champions {

    val log:Logger = LoggerFactory.getLogger(Champions.getClass) ;

    var champions = collection.mutable.HashMap.empty[Int, String]

    val riotApi = new RiotApi(Region.EUW)

    private def populateMap(): Unit = {
        import collection.JavaConverters._

        riotApi.getChampionsList.asScala.foreach(champ => champions(champ.id) = champ.name)
    }

    def byId(id:Int):String = {
        champions.get(id) match {
            case Some(name) => name
            case None =>
                log.debug("Champion id {} not found. Downloading from riot..", id)
                //We can't find the id, load it from riot
                populateMap()
                champions.get(id).getOrElse("UNKNOWN CHAMPION")
        }
    }


    def main(args: Array[String]): Unit = {
        print (Champions.byId(23))
    }
}
