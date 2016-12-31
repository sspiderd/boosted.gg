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
                                FlatArmorMod: Double = 0,
                                FlatAttackSpeedMod: Double = 0,
                                FlatBlockMod: Double = 0,
                                FlatCritChanceMod: Double = 0,
                                FlatCritDamageMod: Double = 0,
                                FlatEXPBonus: Double = 0,
                                FlatEnergyPoolMod: Double = 0,
                                FlatEnergyRegenMod: Double = 0,
                                FlatHPPoolMod: Double = 0,
                                FlatHPRegenMod: Double = 0,
                                FlatMPPoolMod: Double = 0,
                                FlatMPRegenMod: Double = 0,
                                FlatMagicDamageMod: Double = 0,
                                FlatMovementSpeedMod: Double = 0,
                                FlatPhysicalDamageMod: Double = 0,
                                FlatSpellBlockMod: Double = 0,
                                PercentArmorMod: Double = 0,
                                PercentAttackSpeedMod: Double = 0,
                                PercentBlockMod: Double = 0,
                                PercentCritChanceMod: Double = 0,
                                PercentCritDamageMod: Double = 0,
                                PercentDodgeMod: Double = 0,
                                PercentEXPBonus: Double = 0,
                                PercentHPPoolMod: Double = 0,
                                PercentHPRegenMod: Double = 0,
                                PercentLifeStealMod: Double = 0,
                                PercentMPPoolMod: Double = 0,
                                PercentMPRegenMod: Double = 0,
                                PercentMagicDamageMod: Double = 0,
                                PercentMovementSpeedMod: Double = 0,
                                PercentPhysicalDamageMod: Double = 0,
                                PercentSpellBlockMod: Double = 0,
                                PercentSpellVampMod: Double = 0,
                                rFlatArmorModPerLevel: Double = 0,
                                rFlatArmorPenetrationMod: Double = 0,
                                rFlatArmorPenetrationModPerLevel: Double = 0,
                                rFlatCritChanceModPerLevel: Double = 0,
                                rFlatCritDamageModPerLevel: Double = 0,
                                rFlatDodgeMod: Double = 0,
                                rFlatDodgeModPerLevel: Double = 0,
                                rFlatEnergyModPerLevel: Double = 0,
                                rFlatEnergyRegenModPerLevel: Double = 0,
                                rFlatGoldPer10Mod: Double = 0,
                                rFlatHPModPerLevel: Double = 0,
                                rFlatHPRegenModPerLevel: Double = 0,
                                rFlatMPModPerLevel: Double = 0,
                                rFlatMPRegenModPerLevel: Double = 0,
                                rFlatMagicDamageModPerLevel: Double = 0,
                                rFlatMagicPenetrationMod: Double = 0,
                                rFlatMagicPenetrationModPerLevel: Double = 0,
                                rFlatMovementSpeedModPerLevel: Double = 0,
                                rFlatPhysicalDamageModPerLevel: Double = 0,
                                rFlatSpellBlockModPerLevel: Double = 0,
                                rFlatTimeDeadMod: Double = 0,
                                rFlatTimeDeadModPerLevel: Double = 0,
                                rPercentArmorPenetrationMod: Double = 0,
                                rPercentArmorPenetrationModPerLevel: Double = 0,
                                rPercentAttackSpeedModPerLevel: Double = 0,
                                rPercentCooldownMod: Double = 0,
                                rPercentCooldownModPerLevel: Double = 0,
                                rPercentMagicPenetrationMod: Double = 0,
                                rPercentMagicPenetrationModPerLevel: Double = 0,
                                rPercentMovementSpeedModPerLevel: Double = 0,
                                rPercentTimeDeadMod: Double = 0,
                                rPercentTimeDeadModPerLevel: Double = 0
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

        var FlatArmorMod = 0d
        var FlatAttackSpeedMod = 0d
        var FlatBlockMod = 0d
        var FlatCritChanceMod = 0d
        var FlatCritDamageMod = 0d
        var FlatEXPBonus = 0d
        var FlatEnergyPoolMod = 0d
        var FlatEnergyRegenMod = 0d
        var FlatHPPoolMod = 0d
        var FlatHPRegenMod = 0d
        var FlatMPPoolMod = 0d
        var FlatMPRegenMod = 0d
        var FlatMagicDamageMod = 0d
        var FlatMovementSpeedMod = 0d
        var FlatPhysicalDamageMod = 0d
        var FlatSpellBlockMod = 0d
        var PercentArmorMod = 0d
        var PercentAttackSpeedMod = 0d
        var PercentBlockMod = 0d
        var PercentCritChanceMod = 0d
        var PercentCritDamageMod = 0d
        var PercentDodgeMod = 0d
        var PercentEXPBonus = 0d
        var PercentHPPoolMod = 0d
        var PercentHPRegenMod = 0d
        var PercentLifeStealMod = 0d
        var PercentMPPoolMod = 0d
        var PercentMPRegenMod = 0d
        var PercentMagicDamageMod = 0d
        var PercentMovementSpeedMod = 0d
        var PercentPhysicalDamageMod = 0d
        var PercentSpellBlockMod = 0d
        var PercentSpellVampMod = 0d

        chosenItems.foreach(item => {
            FlatArmorMod += item.stats.FlatArmorMod
            FlatAttackSpeedMod += item.stats.FlatAttackSpeedMod
            FlatBlockMod += item.stats.FlatBlockMod
            FlatCritChanceMod += item.stats.FlatCritChanceMod
            FlatCritDamageMod += item.stats.FlatCritDamageMod
            FlatEXPBonus += item.stats.FlatEXPBonus
            FlatEnergyPoolMod += item.stats.FlatEnergyPoolMod
            FlatEnergyRegenMod += item.stats.FlatEnergyRegenMod
            FlatHPPoolMod += item.stats.FlatHPPoolMod
            FlatHPRegenMod += item.stats.FlatHPRegenMod
            FlatMPPoolMod += item.stats.FlatMPPoolMod
            FlatMPRegenMod += item.stats.FlatMPRegenMod
            FlatMagicDamageMod += item.stats.FlatMagicDamageMod
            FlatMovementSpeedMod += item.stats.FlatMovementSpeedMod
            FlatPhysicalDamageMod += item.stats.FlatPhysicalDamageMod
            FlatSpellBlockMod += item.stats.FlatSpellBlockMod
            PercentArmorMod += item.stats.PercentArmorMod
            PercentAttackSpeedMod += item.stats.PercentAttackSpeedMod
            PercentBlockMod += item.stats.PercentBlockMod
            PercentCritChanceMod += item.stats.PercentCritChanceMod
            PercentCritDamageMod += item.stats.PercentCritDamageMod
            PercentDodgeMod += item.stats.PercentDodgeMod
            PercentEXPBonus += item.stats.PercentEXPBonus
            PercentHPPoolMod += item.stats.PercentHPPoolMod
            PercentHPRegenMod += item.stats.PercentHPRegenMod
            PercentLifeStealMod += item.stats.PercentLifeStealMod
            PercentMPPoolMod += item.stats.PercentMPPoolMod
            PercentMPRegenMod += item.stats.PercentMPRegenMod
            PercentMagicDamageMod += item.stats.PercentMagicDamageMod
            PercentMovementSpeedMod += item.stats.PercentMovementSpeedMod
            PercentPhysicalDamageMod += item.stats.PercentPhysicalDamageMod
            PercentSpellBlockMod += item.stats.PercentSpellBlockMod
            PercentSpellVampMod += item.stats.PercentSpellVampMod
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
            FlatArmorMod,
            FlatAttackSpeedMod,
            FlatBlockMod,
            FlatCritChanceMod,
            FlatCritDamageMod,
            FlatEXPBonus,
            FlatEnergyPoolMod,
            FlatEnergyRegenMod,
            FlatHPPoolMod,
            FlatHPRegenMod,
            FlatMPPoolMod,
            FlatMPRegenMod,
            FlatMagicDamageMod,
            FlatMovementSpeedMod,
            FlatPhysicalDamageMod,
            FlatSpellBlockMod,
            PercentArmorMod,
            PercentAttackSpeedMod,
            PercentBlockMod,
            PercentCritChanceMod,
            PercentCritDamageMod,
            PercentDodgeMod,
            PercentEXPBonus,
            PercentHPPoolMod,
            PercentHPRegenMod,
            PercentLifeStealMod,
            PercentMPPoolMod,
            PercentMPRegenMod,
            PercentMagicDamageMod,
            PercentMovementSpeedMod,
            PercentPhysicalDamageMod,
            PercentSpellBlockMod,
            PercentSpellVampMod
        )
    }

}