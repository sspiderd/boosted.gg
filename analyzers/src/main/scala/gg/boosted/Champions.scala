package gg.boosted

import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.io.Source

/**
  * Created by ilan on 8/28/16.
  */
object Champions {

    val championsJsonLocation = "/champions.json"

    var map:Map[Int,String] = Map()

    def jsonStrToMap(jsonStr: String): Map[Int, String] = {
        implicit val formats = org.json4s.DefaultFormats

        val map = parse(jsonStr).extract[Map[Int, String]]
        return map
    }

    def populateMap():Unit = {
        val json = Source.fromInputStream(getClass.getResourceAsStream(championsJsonLocation)).mkString
        map = jsonStrToMap(json)
    }

    def byId(id:Int):String = {
        if (!map.contains(id)) {
            populateMap()
        }
        if (!map.contains(id)) {
            return "UNKNOWN CHAMP"
        }
        return map(id)
    }

    def main(args: Array[String]) {
        populateMap()
    }

}
