package com.andrewberls.sstable

import java.util.ArrayList
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import com.andrewberls.sstable.DiskTable
import com.andrewberls.sstable.MemTable

class SSTable(
    private val duration: Long = 10000,
    private val unit: TimeUnit = TimeUnit.MILLISECONDS)
{
    // Global table lock used to guard memtables/disktables
    // during flush/compaction processes
    private val LOCK = Object()

    private var memtable = MemTable()

    private val disktables = ArrayList<DiskTable>()

    private val memtableFlusherThread = thread {
        while(true) {
            Thread.sleep(unit.toMillis(duration))
            flushMemTable()
        }
    }

    private fun flushMemTable(): Unit {
        // TODO: could be much smarter about critical section here;
        // snapshot, build offline and preserve any writes made in meantime?
        synchronized(LOCK) {
            val flushedTable = DiskTable.build(memtable.entries())
            disktables.add(flushedTable)
            memtable = MemTable()
        }
    }

    // TODO: disktables merge thread

    fun get(k: String): ByteArray? {
        synchronized(LOCK) {
            val mv: ByteArray? = memtable.get(k)
            if (mv != null) {
                return mv
            } else {
                disktables.forEach { table ->
                    val dv = table.get(k)
                    if (dv != null) {
                        return dv
                    }
                }
            }

            return null
        }
    }

    fun put(k: String, v: ByteArray): Unit {
        synchronized(LOCK) { memtable.put(k, v) }
    }
}
