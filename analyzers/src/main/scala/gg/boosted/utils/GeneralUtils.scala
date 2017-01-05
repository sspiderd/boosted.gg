package gg.boosted.utils

import org.slf4j.LoggerFactory

/**
  * Created by ilan on 1/5/17.
  */
object GeneralUtils {

    val log = LoggerFactory.getLogger(GeneralUtils.getClass)

    def time[R](block: => R, blockName:String = ""): R = {
        val t0 = System.currentTimeMillis()
        val result = block    // call-by-name
        val t1 = System.currentTimeMillis()
        log.debug(s"Elapsed time for '${blockName}': ${(t1 - t0)} ms")
        result
    }

}
