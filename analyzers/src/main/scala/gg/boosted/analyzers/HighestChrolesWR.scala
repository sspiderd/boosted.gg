package gg.boosted.analyzers

import gg.boosted.{Chrole, SummonerMatch}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

/**
  *
  * This class gives the highest winrates by chroles
  *
  * Created by ilan on 8/25/16.
  */
object HighestChrolesWR {


    /**
      * Calculate the Chroles with the highest winrate, without additional filtering
      * @param ds
      * @return
      */
    //def raw(ds: Dataset[SummonerMatch]):Dataset[(Chrole, Float)] = {
        //ds.select("Region").
    //}

}
