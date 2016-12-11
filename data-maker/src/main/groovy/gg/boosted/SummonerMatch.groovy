package gg.boosted

import groovy.transform.CompileStatic

/**
 * Created by ilan on 8/11/16.
 */
@CompileStatic
class SummonerMatch {

    Long matchId
    Long summonerId
    Integer championId
    Integer roleId
    Boolean winner
    String region
    Long date
}
