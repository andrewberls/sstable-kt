package com.andrewberls.sstable

import kotlin.test.*
import org.junit.Test
import kotlin.io.use
import com.andrewberls.sstable.SSTable
import com.andrewberls.sstable.TestUtils.intToByteArray
import com.andrewberls.sstable.TestUtils.byteArrayToInt

class SSTableTest {
    @Test
    fun testGetFromMemTable(): Unit {
        val table = SSTable(100, 10000)
        table.use {
            table.put("a", intToByteArray(1))
            table.put("b", intToByteArray(2))
            assertEquals(1, byteArrayToInt(table.get("a")!!))
            assertEquals(2, byteArrayToInt(table.get("b")!!))
            assertEquals(null, table.get("missing"))
        }
    }

    @Test
    fun testGetFromDiskTable(): Unit {
        val table = SSTable(3, 500)
        table.use {
            table.put("a", intToByteArray(1))
            table.put("b", intToByteArray(2))
            table.put("c", intToByteArray(3))
            Thread.sleep(500) // Await flush trigger
            assertEquals(3, byteArrayToInt(table.getFromDisk("c")!!))
            assertEquals(2, byteArrayToInt(table.getFromDisk("b")!!))
            assertEquals(1, byteArrayToInt(table.getFromDisk("a")!!))
            assertEquals(null, table.getFromDisk("missing"))
        }
    }
}
