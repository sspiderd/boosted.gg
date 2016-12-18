package gg.boosted.posos

import org.slf4j.LoggerFactory

/**
  * Created by ilan on 9/9/16.
  */
case class LoLScore(tier:String, division: String, leaguePoints:Int, lolScore:Int)

object LoLScore {

    val log = LoggerFactory.getLogger(LoLScore.getClass)

    def apply(tier: String, division: String, leaguePoints:Int):LoLScore = {
        var lolScore = 0
        if (tier == "UNRANKED") {
            return LoLScore("UNRANKED", "U", 0, -1)
        }

        tier match {
            case "UNRANKED" => 0
            case "BRONZE" => 0
            case "SILVER" => lolScore += 500
            case "GOLD" => lolScore += 1000
            case "PLATINUM" => lolScore += 1500
            case "DIAMOND" => lolScore += 2000
            case "MASTER" | "CHALLENGER" => lolScore += 2500
            case _ => {
                log.error(s"Unknown league: $tier")
                return LoLScore("UNKNOWN", "U", 0, -1)
            }
        }
        if (tier != "MASTER" && tier != "CHALLENGER") {
            division match {
                case "V" => 0
                case "IV" => lolScore += 100
                case "III" => lolScore += 200
                case "II" => lolScore += 300
                case "I" => lolScore += 400
                case "U" => 0
                case _ => {
                    log.error(s"Unknown division: $division")
                    return LoLScore("UNKNOWN", "U", 0, -1)
                }
            }
        }

        lolScore += leaguePoints
        LoLScore(tier, division, leaguePoints, lolScore)
    }

}
