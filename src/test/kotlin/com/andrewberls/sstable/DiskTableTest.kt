package com.andrewberls.sstable

import java.io.RandomAccessFile
import kotlin.test.*
import org.junit.Test
import com.andrewberls.sstable.Record
import com.andrewberls.sstable.Utils
import com.andrewberls.sstable.TestUtils.intToByteArray
import com.andrewberls.sstable.TestUtils.byteArrayToInt

class DiskTableTest {
    @Test
    fun testCompanionBuild(): Unit {
        val kvs = listOf(Pair("a", Record.Value(intToByteArray(1))),
                         Pair("b", Record.Value(intToByteArray(2))),
                         Pair("c", Record.Value(intToByteArray(3))))
        val dt = DiskTable.build(kvs)
        val raf = dt.raf
        raf.seek(0)

        // Header
        assertEquals(50, raf.readLong()) // Index offset

        // KVs
        assertEquals("a", Utils.readLengthPrefixedString(raf))
        assertEquals(1, raf.readByte().toInt())
        assertEquals(4, raf.readInt()) // 1 length
        assertEquals(1, raf.readInt()) // 1 (byte hack)

        assertEquals("b", Utils.readLengthPrefixedString(raf))
        assertEquals(1, raf.readByte().toInt())
        assertEquals(4, raf.readInt()) // 2 length
        assertEquals(2, raf.readInt()) // 2 (byte hack)

        assertEquals("c", Utils.readLengthPrefixedString(raf))
        assertEquals(1, raf.readByte().toInt())
        assertEquals(4, raf.readInt()) // 3 length
        assertEquals(3, raf.readInt()) // 3 (byte hack)

        // Index
        assertEquals("a", Utils.readLengthPrefixedString(raf))
        assertEquals(13, raf.readLong())

        assertEquals("b", Utils.readLengthPrefixedString(raf))
        assertEquals(27, raf.readLong())

        assertEquals("c", Utils.readLengthPrefixedString(raf))
        assertEquals(41, raf.readLong())

        // Nothing else should be present
        assertEquals(raf.length(), raf.getFilePointer())

        raf.close()
    }

    @Test
    fun testBuildIndex(): Unit {
        val kvs = listOf(Pair("a", Record.Value(intToByteArray(1))),
                         Pair("b", Record.Value(intToByteArray(2))),
                         Pair("c", Record.Value(intToByteArray(3))))
        val dt = DiskTable.build(kvs)

        val expected = hashMapOf(Pair("a", 13.toLong()),
                                 Pair("b", 27.toLong()),
                                 Pair("c", 41.toLong()))
        val actual = dt.buildIndex()
        assertEquals(expected.get("a")!! as Long, actual.get("a")!! as Long)
        assertEquals(expected.get("b")!! as Long, actual.get("b")!! as Long)
        assertEquals(expected.get("c")!! as Long, actual.get("c")!! as Long)
    }

    @Test
    fun testGet(): Unit {
        val kvs = listOf(Pair("a", Record.Value(intToByteArray(1))),
                         Pair("b", Record.Value(intToByteArray(2))),
                         Pair("c", Record.Value(intToByteArray(3))))
        val dt = DiskTable.build(kvs)

        assertEquals(1, byteArrayToInt(dt.get("a")!!))
        assertEquals(2, byteArrayToInt(dt.get("b")!!))
        assertEquals(3, byteArrayToInt(dt.get("c")!!))

        assertEquals(null, dt.get("missing"))
    }
}
