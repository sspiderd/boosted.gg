package gg.boosted.analyzers

import gg.boosted.posos.Chrole
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer

/**
  * Created by ilan on 8/27/16.
  */
object DataFrameUtils {

    def findDistinctChampionAndRoleIds(df:DataFrame):List[Chrole] = {
        val chroles = new ListBuffer[Chrole]
        df.select("championId", "roleId").distinct().collect().foreach(row => {
            chroles += new Chrole(row.get(0).asInstanceOf[Int], row.get(1).asInstanceOf[Int])
        })
        return chroles.toList
    }

}
