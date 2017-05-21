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
    Integer winner
    String region
    Long creationDate
    Integer patchMajorVersion
    Integer patchMinorVersion
}
