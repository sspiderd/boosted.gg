package gg.boosted.maps

import gg.boosted.{Region, RiotApi}

/**
  * Created by ilan on 12/9/16.
  */
object Champions {

    var champions = collection.mutable.HashMap.empty[Int, String]

    private def populateMap(): Unit = {
        import collection.JavaConverters._
        val riotApi = new RiotApi(Region.EUW)
        riotApi.getChampionsList.asScala.foreach(champ => champions(champ.id) = champ.name)
    }

    def byId(id:Int):String = {
        champions.get(id) match {
            case Some(name) => name
            case None =>
                //We can't find the id, load it from riot
                populateMap()
                champions.get(id).getOrElse("UNKNOWN CHAMPION")
        }
    }


    def main(args: Array[String]): Unit = {
        print (Champions.byId(23))
    }
}
