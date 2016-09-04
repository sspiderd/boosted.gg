package gg.boosted.analyzers

import gg.boosted.posos.Chrole
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer

/**
  * Created by ilan on 8/27/16.
  */
object DataFrameUtils {

    def findDistinctChampionAndRoleIds(df:DataFrame):Array[Chrole] = {
        df.select("championId", "roleId").distinct().collect().map(row => Chrole(row.getInt(0), row.getInt(1)))
    }

}
