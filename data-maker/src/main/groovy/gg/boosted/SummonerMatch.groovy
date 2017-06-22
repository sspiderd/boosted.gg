package gg.boosted

import groovy.transform.CompileStatic

/**
 * Created by ilan on 8/11/16.
 */
@CompileStatic
class SummonerMatch {

    Integer championId
    Integer roleId
    Long gameId
    Long summonerId
    Integer winner
    String platformId
    Long creationDate
    Integer patchMajorVersion
    Integer patchMinorVersion
}
