package gg.boosted.maps

import gg.boosted.riotapi.{Region, RiotApi}

/**
  * Created by ilan on 12/16/16.
  */
object RiotApis {

    val map = collection.mutable.HashMap.empty[Region, RiotApi]

    def get(region:Region):RiotApi = {
        map.get(region) match {
            case Some(api) => api
            case None => {
                map.synchronized {
                    map.getOrElse(region, {
                        //Create an api for all regions
                        Region.values().foreach(region => map(region) = new RiotApi(region))
                        map.get(region).get
                    })
                }
            }
        }
    }

}
