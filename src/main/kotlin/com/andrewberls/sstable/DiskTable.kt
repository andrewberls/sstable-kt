package com.andrewberls.sstable

import java.io.RandomAccessFile
import java.io.EOFException
import java.util.ArrayList
import java.util.HashMap
import kotlin.collections.List
import com.andrewberls.sstable.Utils

/**
 * A key-value set stored in sorted order on disk, along with an index
 * of file pointer offsets for each key. Materializes the index into
 * memory when constructed.
 *
 * Header:
 *     index offset: long (8)
 * KVs...
 *     k length: int (4)
 *     k bytes (x)
 *     v length: long (8)
 *     v bytes (x)
 * Index
 *     k length: int (4)
 *     k bytes (x)
 *     v offset: long (8)
 */
class DiskTable(val raf: RandomAccessFile) {
    companion object {
        // Invariant: kvs presorted in ascending order
        // TODO: check/enforce this
        fun build(kvs: List<Pair<String, ByteArray>>): DiskTable {
            val raf = RandomAccessFile(Utils.tempfile(), "rws")
            val index = ArrayList<Pair<String, Long>>()

            // Make room for headers (index offset: Long)
            raf.seek(8)

            // Write KVs
            kvs.forEach { p ->
                val (k, v) = p
                raf.writeInt(k.length)
                raf.writeBytes(k)

                index.add(Pair(k, raf.getFilePointer()))
                raf.writeLong(v.size.toLong())
                raf.write(v)
            }

            // Jump back and write index offset header
            val indexStart = raf.getFilePointer()
            raf.seek(0)
            raf.writeLong(indexStart)
            raf.seek(indexStart)

            // Write index
            index.forEach { p ->
                val (k, offset) = p
                raf.writeInt(k.length)
                raf.writeBytes(k)
                raf.writeLong(offset)
            }

            raf.seek(0)
            return DiskTable(raf)
        }
    }

    private val index = buildIndex()

    internal fun buildIndex(): Map<String, Long> {
        raf.seek(0)
        val idx = HashMap<String, Long>()
        val indexStart = raf.readLong()
        raf.seek(indexStart)
        while(true) {
            try {
                val k = Utils.readLengthPrefixedString(raf)
                val offset = raf.readLong()
                idx.put(k, offset)
            } catch (e: EOFException) { break }
        }
        return idx
    }

    fun get(k: String): ByteArray? {
        val offset = index.get(k)
        if (offset != null) {
            raf.seek(offset)
            val len = raf.readLong()
            val v = ByteArray(Math.toIntExact(len)) // TODO: fix format
            raf.read(v)
            return v
        } else {
            return null
        }
    }
}
