package gg.boosted.maps

import gg.boosted.Application
import gg.boosted.riotapi.{Platform, RiotApi}
import gg.boosted.riotapi.dtos.{Mastery, RuneDef}
import org.apache.spark.broadcast.Broadcast
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by ilan on 2/6/17.
  */
object Masteries {

  val log:Logger = LoggerFactory.getLogger(Masteries.getClass) ;

  var masteriesBr:Broadcast[Map[String, Mastery]] = _

  val riotApi = new RiotApi(Platform.EUW1)

  def populateAndBroadcast():Unit = {
    import collection.JavaConverters._
    val masteries = riotApi.getMasteries.asScala.map(mastery => (mastery.id, mastery)).toMap
    masteriesBr = Application.session.sparkContext.broadcast(masteries)
  }

  def masteries():Map[String, Mastery] = {
    masteriesBr.value
  }

  def byId(id:String):Mastery = {
    masteries().get(id) match {
      case Some(mastery) => mastery
      case None =>
        log.debug("Item id {} not found. Downloading from riot..", id)
        //We can't find the id, load it from riot
        populateAndBroadcast()
        masteries.get(id).get
    }
  }

  /**
    * I don't really have the height of the mastery, but as it seems right now, it goes like this
    * mastery ID:
    * First char: always seems to be 6 (season?)
    * Second char: Which mastery tree
    * Third char: The height
    * Fourth char: Position of the mastery in the level
    *
    * So i'm just returning the third char for now
    * @param masteryId
    * @return
    */
  def height(masteryId:String):Int = {
    masteryId.charAt(2)
  }

  def tree(masteryId:String):Int = {
    masteryId.charAt(1)
  }


}
