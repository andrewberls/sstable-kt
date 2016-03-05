package com.andrewberls.sstable

import java.nio.ByteBuffer

object TestUtils {
    fun intToByteArray(x: Int): ByteArray {
        val buf = ByteBuffer.allocate(4)
        buf.putInt(x)
        buf.rewind()
        return buf.array()
    }

    fun byteArrayToInt(bs: ByteArray): Int {
        val buf = ByteBuffer.wrap(bs)
        return buf.getInt()
    }
}
