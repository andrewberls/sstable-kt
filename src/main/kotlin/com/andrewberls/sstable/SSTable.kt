package com.andrewberls.sstable

import java.io.Closeable
import java.util.ArrayList
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.thread
import kotlin.concurrent.withLock
import com.andrewberls.sstable.DiskTable
import com.andrewberls.sstable.MemTable
import com.andrewberls.sstable.Record
import com.andrewberls.sstable.Utils

/**
 * A Sorted String Table (SSTable) efficiently stores large numbers
 * of key-value pairs. All writes go to an in-memory table (MemTable) which
 * is also first in line for reads, and is periodically flushed to disk.
 * In-memory indexes are maintained for tables on disk, so reads only require
 * a single disk seek; a worst-case read for a missing key requires N disk seeks,
 * where N is the current number of disk tables (disk tables are periodically compacted
 * together)
 *
 * @param memCapacity - Number of K/Vs to store in memory before triggering a flush
*                       (flushing based on time; capacity is approximate)
 * @param flushPeriodMillis - Period between checking if MemTable needs flushing to disk
 * @param diskTablesThresh - Number of separate DiskTables to maintain before compaction
 *                           (compaction based on time; limit is approximate)
 * @param compactPeriodMillis - Period between checking if DiskTables need compaction
 *
 * MemTable flushing and DiskTable compaction is performed on separate threads;
 * call `.close()` to cleanly shut down all threaded components.
 *
 * See https://www.igvita.com/2012/02/06/sstable-and-log-structured-storage-leveldb/
 *     https://en.wikipedia.org/wiki/Log-structured_merge-tree
 */
class SSTable(
        private val memCapacity: Long = 10000,
        private val flushPeriodMillis: Long = 1000,
        private val diskTablesThresh: Int = 4,
        private val compactPeriodMillis: Long = 10000) : Closeable {
    private val RWLOCK = ReentrantReadWriteLock()

    // The main MemTable that receives all writes and is first
    // in line for reads
    private var memtable = MemTable(memCapacity)

    // A temporary/transient MemTable that holds the old memtable while
    // flushing/building a new DiskTable and swapping it in
    private var stagingMemtable: MemTable? = null

    private val disktables = ArrayList<DiskTable>()

    private val memtableFlusherThread = thread {
        Utils.foreverInterruptible {
            val atCapacity = RWLOCK.readLock().withLock {
                memtable.atCapacity()
            }
            if (atCapacity) {
                RWLOCK.writeLock().withLock { flushMemTable() }
            }
            Thread.sleep(flushPeriodMillis)
        }
    }

    private val compactionThread = thread {
        Utils.foreverInterruptible {
            val needCompaction = RWLOCK.readLock().withLock {
                disktables.size >= diskTablesThresh
            }
            if (needCompaction) {
                RWLOCK.writeLock().withLock { compactDiskTables() }
            }
            Thread.sleep(compactPeriodMillis)
        }
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

   private fun compactDiskTables(): Unit {
       TODO("compactDiskTables")
   }

   internal fun getFromDisk(k: String): ByteArray? {
       disktables.forEach { table ->
           val v = table.get(k)
           if (v != null) { return v }
       }
       return null
   }

   fun containsKey(k: String): Boolean =
       get(k) != null

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
           if (containsKey(k)) {
               memtable.putRecord(k, Record.Tombstone)
           }
       }
   }

   override fun close(): Unit {
       memtableFlusherThread.interrupt()
       compactionThread.interrupt()
   }
}
