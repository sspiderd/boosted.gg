package gg.boosted

import groovy.json.JsonOutput
import org.msgpack.core.MessageBufferPacker
import org.msgpack.core.MessagePack

/**
 * Created by ilan on 8/17/16.
 */
class MessagePacker {

    static byte[] pack(SummonerMatch summonerMatch) {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker()
        String json = JsonOutput.toJson(summonerMatch)
        packer.packString(json)
        packer.close()
        return packer.toByteArray()

    }

}
