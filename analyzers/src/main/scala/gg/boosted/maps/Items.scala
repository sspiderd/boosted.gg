package gg.boosted.maps

import gg.boosted.Application
import gg.boosted.riotapi.dtos.Item
import gg.boosted.riotapi.{Region, RiotApi}
import org.apache.spark.broadcast.Broadcast
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by ilan on 12/27/16.
  */

case class ItemWeights(attackDamage:Double, abilityPower:Double, armor:Double, magicResistance:Double, health:Double,
                       mana: Double, healthRegen:Double, manaRegen:Double, criticalStrikeChance:Double, attackSpeed:Double,
                       flatMovementSpeed:Double, lifeSteal:Double, percentMovementSpeed:Double)
object Items {
    val log:Logger = LoggerFactory.getLogger(Items.getClass) ;

    val legendaryCutoff = 1600

    val advancedCutoff = 800

    var itemsBr:Broadcast[Map[String,Item]] = _

    def populateAndBroadcast():Unit = {
        import collection.JavaConverters._
        val items = new RiotApi(Region.EUW).getItems.asScala.toMap
        itemsBr = Application.session.sparkContext.broadcast(items)
    }

    def items():Map[String,Item] = {
        itemsBr.value
    }


    def byId(id:String):Item = {
        items().get(id) match {
            case Some(item) => item
            case None =>
                log.debug("Item id {} not found. Downloading from riot..", id)
                //We can't find the id, load it from riot
                populateAndBroadcast()
                items.get(id).get
        }
    }

    def legendaries():Map[String, Item] = {
        items().filter(_._2.gold >= legendaryCutoff)
    }

    def advanced():Map[String, Item] = {
        items().filter(item => item._2.gold < legendaryCutoff && item._2.gold >= advancedCutoff)
    }


    /**
      * Returns the scaled stats of the item, weighted by gold (YEA!)
      * Numbers taken from: http://leagueoflegends.wikia.com/wiki/Gold_efficiency
      * @param item
      * @return
      */
    def weights(item:Item):ItemWeights = {
        val stats = item.stats
        ItemWeights(
            stats.FlatPhysicalDamageMod * 35,
            stats.FlatMagicDamageMod * 21.75,
            stats.FlatArmorMod * 20,
            stats.FlatSpellBlockMod * 18,
            stats.FlatHPPoolMod * 2.666,
            stats.FlatMPPoolMod * 1.4,
            stats.FlatHPRegenMod * 3,
            stats.FlatMPRegenMod * 5,
            stats.FlatCritChanceMod * 40 * 100,
            stats.PercentAttackSpeedMod * 25 * 100,
            stats.FlatMovementSpeedMod * 12,
            stats.PercentLifeStealMod * 37.5 * 100,
            stats.PercentMovementSpeedMod * 39.5 * 100
        )
    }

    /**
      * Returns the accumulated weight of 2 runes
      * @param item1
      * @param item2
      * @return
      */
    def accumulatedWeight(item1:ItemWeights, item2:ItemWeights):ItemWeights = {
        ItemWeights(
            item1.attackDamage + item2.attackDamage,
            item1.abilityPower + item2.abilityPower,
            item1.armor + item2.armor,
            item1.magicResistance + item2.magicResistance,
            item1.health + item2.health,
            item1.mana + item2.mana,
            item1.healthRegen + item2.healthRegen,
            item1.manaRegen + item2.manaRegen,
            item1.attackSpeed + item2.attackSpeed,
            item1.criticalStrikeChance + item2.criticalStrikeChance,
            item1.flatMovementSpeed + item2.flatMovementSpeed,
            item1.lifeSteal + item2.lifeSteal,
            item1.percentMovementSpeed + item2.percentMovementSpeed
        )
    }


    def main(args: Array[String]): Unit = {
        print (Items.byId("2010").name)
    }
}