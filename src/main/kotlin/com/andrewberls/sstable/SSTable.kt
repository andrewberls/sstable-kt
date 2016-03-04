package com.andrewberls.sstable

import java.util.ArrayList
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.*
import com.andrewberls.sstable.DiskTable
import com.andrewberls.sstable.MemTable
import com.andrewberls.sstable.Record

class SSTable(
        private val memCapacity: Long = 10000,
        private val flushPeriodMillis: Long = 1000) {
    private val RWLOCK = ReentrantReadWriteLock()

    private var memtable = MemTable(memCapacity)

    private val disktables = ArrayList<DiskTable>()

    private val memtableFlusherThread = thread {
        try {
            while(true) {
                val atCapacity = RWLOCK.readLock().withLock {
                    memtable.atCapacity()
                }
                if (atCapacity) {
                    RWLOCK.writeLock().withLock { flushMemTable() }
                }
                Thread.sleep(flushPeriodMillis)
            }
        } catch (e: InterruptedException) {}
    }

    private fun flushMemTable(): Unit {
        // TODO: poor throughput here
        RWLOCK.writeLock().withLock {
            val flushedTable = DiskTable.build(memtable.entries())
            disktables.add(flushedTable)
            this.memtable = MemTable(memCapacity)
        }
   }

    // TODO: disktable compaction. count flushes?

    fun get(k: String): ByteArray? {
        RWLOCK.readLock().withLock {
            val mv: ByteArray? = memtable.get(k)
            if (mv != null) {
                return mv
            } else {
                disktables.forEach { table ->
                    val dv = table.get(k)
                    if (dv != null) { return dv }
                }
            }

            return null
        }
    }

    fun put(k: String, v: ByteArray): Unit {
        RWLOCK.writeLock().withLock {
            memtable.putRecord(k, Record.Value(v))
        }
    }

    fun remove(k: String): Unit {
        RWLOCK.writeLock().withLock {
            memtable.putRecord(k, Record.Tombstone)
        }
    }

    fun close(): Unit {
        memtableFlusherThread.interrupt()
    }
}
