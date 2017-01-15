package gg.boosted.posos

/**
  * Created by ilan on 1/4/17.
  */
case class SummonerMatchSummary(
                                   matchId: Long,
                                   summonerId: Long,
                                   region: String,
                                   championId: Int,
                                   roleId: Int,
                                   runes: Seq[Int],
                                   masteries: Seq[Int],
                                   boughtItems: Seq[Int],
                                   winner: Boolean
                               )
