package com.andrewberls.sstable

import java.util.ArrayList
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.* // withLock
import com.andrewberls.sstable.DiskTable
import com.andrewberls.sstable.MemTable
import com.andrewberls.sstable.Record

class SSTable(private val memCapacity: Long = 10000) {
    private val RWLOCK = ReentrantReadWriteLock()

    private var memtable = MemTable(memCapacity)

    private val disktables = ArrayList<DiskTable>()

    // TODO: disktable compaction. count flushes?

    fun get(k: String): ByteArray? {
        RWLOCK.readLock().withLock {
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

    private fun flushMemTable(): Unit {
        RWLOCK.writeLock().withLock {
            val flushedTable = DiskTable.build(memtable.entries())
            disktables.add(flushedTable)
            memtable = MemTable(memCapacity)
        }
    }

    fun put(k: String, v: ByteArray): Unit {
        RWLOCK.writeLock().withLock {
            memtable.putRecord(k, Record.Value(v))
            if (memtable.atCapacity()) { flushMemTable() }
        }
    }

    fun remove(k: String): Unit {
        RWLOCK.writeLock().withLock {
            memtable.putRecord(k, Record.Tombstone)
        }
    }
}
