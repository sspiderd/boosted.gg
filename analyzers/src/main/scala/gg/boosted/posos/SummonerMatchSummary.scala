package gg.boosted.posos

import gg.boosted.maps.Items
import gg.boosted.riotapi.dtos.Item

/**
  *
  * The runes, masteries, and first 2 items summary
  *
  * Created by ilan on 12/29/16.
  */
case class SummonerMatchSummary(matchId: Long,
                                summonerId: Long,
                                championId: Int,
                                roleId: Int,
                                winner: Boolean,
                                region: String,
                                date: Long,
                                items: Seq[Int],
                                chosenItems: Seq[Int],
                                WeightedAttackDamage: Double = 0,
                                WeightedAbilityPower: Double = 0,
                                WeightedArmorMod: Double = 0,
                                WeightedMagicResistance: Double = 0,
                                WeightedHealth: Double = 0,
                                WeightedMana:Double = 0,
                                WeightedHealthRegen:Double = 0,
                                WeightedManaRegen:Double = 0,
                                WeightedCriticalStrikeChance: Double = 0,
                                WeightedAttackSpeed: Double = 0,
                                WeightedFlatMovementSpeed: Double = 0,
                                WeightedLifeSteal: Double = 0,
                                WeightedPercentMovementSpeed: Double = 0
                               )


object SummonerMatchSummary {

    def apply(matchId: Long,
              summonerId: Long,
              championId: Int,
              roleId: Int,
              winner: Boolean,
              region: String,
              date: Long,
              items: Seq[Int]): SummonerMatchSummary = {

        val numberOfLegendaryItems = 2
        //Take the first N legendary items
        val chosenItems = items.map(Items.items.get(_).get).filter(_.gold >= Items.legendaryCutoff).take(numberOfLegendaryItems)

        var WeightedAttackDamage = 0d
        var WeightedAbilityPower = 0d
        var WeightedArmorMod = 0d
        var WeightedMagicResistance = 0d
        var WeightedHealth = 0d
        var WeightedMana = 0d
        var WeightedHealthRegen = 0d
        var WeightedManaRegen = 0d
        var WeightedCriticalStrikeChance = 0d
        var WeightedAttackSpeed = 0d
        var WeightedFlatMovementSpeed = 0d
        var WeightedLifeSteal = 0d
        var WeightedPercentMovementSpeed = 0d


        //We weight the stats by gold efficiency from this table:
        //http://leagueoflegends.wikia.com/wiki/Gold_efficiency
        chosenItems.foreach(item => {
            WeightedAttackDamage += item.stats.FlatPhysicalDamageMod * 35
            WeightedAbilityPower += item.stats.FlatMagicDamageMod * 21.75
            WeightedArmorMod += item.stats.FlatArmorMod * 20
            WeightedMagicResistance += item.stats.FlatSpellBlockMod * 18
            WeightedHealth += item.stats.FlatHPPoolMod * 2.666
            WeightedMana += item.stats.FlatMPPoolMod * 1.4
            WeightedHealthRegen += item.stats.FlatHPRegenMod * 3
            WeightedManaRegen += item.stats.FlatMPRegenMod * 5
            WeightedCriticalStrikeChance += item.stats.FlatCritChanceMod * 40 * 100
            WeightedAttackSpeed += item.stats.PercentAttackSpeedMod * 25 * 100
            WeightedFlatMovementSpeed += item.stats.FlatMovementSpeedMod * 12
            WeightedLifeSteal += item.stats.PercentLifeStealMod * 37.5 * 100
            WeightedPercentMovementSpeed += item.stats.PercentMovementSpeedMod * 39.5 * 100
        })

        SummonerMatchSummary(
            matchId,
            summonerId,
            championId,
            roleId,
            winner,
            region,
            date,
            items,
            chosenItems.map(_.id.toInt),
            WeightedAttackDamage,
            WeightedAbilityPower,
            WeightedArmorMod,
            WeightedMagicResistance,
            WeightedHealth,
            WeightedMana,
            WeightedHealthRegen,
            WeightedManaRegen,
            WeightedCriticalStrikeChance,
            WeightedAttackSpeed,
            WeightedFlatMovementSpeed,
            WeightedLifeSteal,
            WeightedPercentMovementSpeed
        )
    }

}