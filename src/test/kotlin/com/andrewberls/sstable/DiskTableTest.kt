package com.andrewberls.sstable

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.util.ArrayList
import kotlin.test.*
import org.junit.Test
import com.andrewberls.sstable.Utils

private fun intToByteArray(x: Int): ByteArray {
    val buf = ByteBuffer.allocate(4)
    buf.putInt(x)
    buf.rewind()
    return buf.array()
}

private fun byteArrayToInt(x: ByteArray): Int {
    val buf = ByteBuffer.wrap(x)
    return buf.getInt()
}

class DiskTableTest {
    @Test
    fun testCompanionBuild(): Unit {
        val kvs = listOf(Pair("a", intToByteArray(1)),
                         Pair("b", intToByteArray(2)),
                         Pair("c", intToByteArray(3)))
        val dt = DiskTable.build(kvs)
        val raf = dt.raf
        raf.seek(0)

        // Header
        assertEquals(47, raf.readLong()) // Index offset

        // KVs
        assertEquals("a", Utils.readLengthPrefixedString(raf))
        assertEquals(4, raf.readInt()) // 1 length
        assertEquals(1, raf.readInt()) // 1 (byte hack)

        assertEquals("b", Utils.readLengthPrefixedString(raf))
        assertEquals(4, raf.readInt()) // 2 length
        assertEquals(2, raf.readInt()) // 2 (byte hack)

        assertEquals("c", Utils.readLengthPrefixedString(raf))
        assertEquals(4, raf.readInt()) // 3 length
        assertEquals(3, raf.readInt()) // 3 (byte hack)

        // Index
        assertEquals("a", Utils.readLengthPrefixedString(raf))
        assertEquals(13, raf.readLong())

        assertEquals("b", Utils.readLengthPrefixedString(raf))
        assertEquals(26, raf.readLong())

        assertEquals("c", Utils.readLengthPrefixedString(raf))
        assertEquals(39, raf.readLong())

        // Nothing else should be present
        assertEquals(raf.length(), raf.getFilePointer())

        raf.close()
    }

    @Test
    fun testBuildIndex(): Unit {
        val kvs = listOf(Pair("a", intToByteArray(1)),
                         Pair("b", intToByteArray(2)),
                         Pair("c", intToByteArray(3)))
        val dt = DiskTable.build(kvs)

        val expected = hashMapOf(Pair("a", 13.toLong()),
                                 Pair("b", 26.toLong()),
                                 Pair("c", 39.toLong()))
        val actual = dt.buildIndex()
        assertEquals(expected.get("a")!! as Long, actual.get("a")!! as Long)
        assertEquals(expected.get("b")!! as Long, actual.get("b")!! as Long)
        assertEquals(expected.get("c")!! as Long, actual.get("c")!! as Long)
    }

    @Test
    fun testGet(): Unit {
        val kvs = listOf(Pair("a", intToByteArray(1)),
                         Pair("b", intToByteArray(2)),
                         Pair("c", intToByteArray(3)))
        val dt = DiskTable.build(kvs)

        assertEquals(1, byteArrayToInt(dt.get("a")!!))
        assertEquals(2, byteArrayToInt(dt.get("b")!!))
        assertEquals(3, byteArrayToInt(dt.get("c")!!))

        assertEquals(null, dt.get("missing"))
    }
}
