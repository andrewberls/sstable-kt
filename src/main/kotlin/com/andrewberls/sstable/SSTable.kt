package com.andrewberls.sstable

import java.io.Closeable
import java.util.ArrayList
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.thread
import kotlin.concurrent.withLock
import com.andrewberls.sstable.DiskTable
import com.andrewberls.sstable.MemTable
import com.andrewberls.sstable.Record

class SSTable(
        private val memCapacity: Long = 10000,
        private val flushPeriodMillis: Long = 1000) : Closeable {
    private val RWLOCK = ReentrantReadWriteLock()

    // The main MemTable that receives all writes and is first
    // in line for reads
    private var memtable = MemTable(memCapacity)

    // A temporary/transient MemTable that holds the old memtable while
    // flushing/building a new DiskTable and swapping it in
    private var stagingMemtable: MemTable? = null

    private val disktables = ArrayList<DiskTable>()

    private val memtableFlusherThread = thread {
        try {
            while (true) {
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

    internal fun getFromDisk(k: String): ByteArray? {
        disktables.forEach { table ->
            val v = table.get(k)
            if (v != null) { return v }
        }
        return null
    }

    fun get(k: String): ByteArray? =
        RWLOCK.readLock().withLock {
            memtable.get(k) ?: stagingMemtable?.get(k) ?: getFromDisk(k)
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

    override fun close(): Unit {
        memtableFlusherThread.interrupt()
    }
}
