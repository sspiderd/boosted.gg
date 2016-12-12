package gg.boosted.posos

import gg.boosted.riotapi.Region

/**
  *
  * Since ids are not unique across regions, an id is the set (id, region)
  *
  * Created by ilan on 12/12/16.
  */
case class SummonerId(id: Long, region: Region)
