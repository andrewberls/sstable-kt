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

    // The main MemTable that receives all writes and is first
    // in line for reads
    private var memtable = MemTable(memCapacity)

    // A temporary/transient MemTable that holds the old memtable while flushing/building
    // a new DiskTable and swapping it in
    private var stagingMemtable: MemTable? = null

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
        RWLOCK.writeLock().withLock {
            stagingMemtable = memtable.copy()
            memtable = MemTable(memCapacity)
        }
        // Build the disk table outside of any locks
        val flushedTable = DiskTable.build(stagingMemtable!!.entries())
        RWLOCK.writeLock().withLock {
            disktables.add(flushedTable)
            stagingMemtable = null
        }
   }

    // TODO: disktable compaction. count flushes?

    fun get(k: String): ByteArray? {
        RWLOCK.readLock().withLock {
            val mv: ByteArray? = memtable.get(k) ?: stagingMemtable?.get(k)
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
