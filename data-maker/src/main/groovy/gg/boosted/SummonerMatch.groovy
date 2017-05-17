package gg.boosted

import groovy.transform.CompileStatic

/**
 * Created by ilan on 8/11/16.
 */
@CompileStatic
class SummonerMatch {

    Integer championId
    Integer roleId
    Long matchId
    Long summonerId
    Byte winner
    String region
    Long creationDate
}
