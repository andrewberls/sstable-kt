package com.andrewberls.sstable

import java.io.EOFException
import java.io.RandomAccessFile
import java.util.ArrayList
import java.util.HashMap
import kotlin.collections.List
import com.andrewberls.sstable.Record
import com.andrewberls.sstable.Utils

/**
 * A set of key-value pairs stored in sorted order on disk, along with an index
 * of file pointer offsets for each key. Materializes the index into
 * memory when constructed.
 *
 * Type tags: 0 = tombstone, 1 = value
 *
 * Disk layout:
 *
 *   Header:
 *       index offset: long (8)
 *   KVs...
 *       k length: int (4)
 *       k: bytes (x)
 *       type tag: byte (1)
 *       If not tombstone:
 *           v length: int (4)
 *           v: bytes (x)
 *   Index
 *       k length: int (4)
 *       k: bytes (x)
 *       type tag offset: long (8)
 */
class DiskTable(val raf: RandomAccessFile) {
    companion object {
        // Invariant: kvs presorted in ascending order
        // TODO: check/enforce this
        fun build(kvs: List<Pair<String, Record>>): DiskTable {
            val raf = RandomAccessFile(Utils.tempfile(), "rw")
            val index = ArrayList<Pair<String, Long>>()

            // Make room for headers (index offset: Long)
            raf.seek(8)

            // Write KVs
            kvs.forEach { p ->
                val (k, r) = p
                when (r) {
                    is Record.Tombstone -> {
                        Utils.writeLengthPrefixedString(raf, k)
                        index.add(Pair(k, raf.getFilePointer()))
                        raf.writeByte(0)
                    }
                    is Record.Value -> {
                        Utils.writeLengthPrefixedString(raf, k)
                        index.add(Pair(k, raf.getFilePointer()))
                        raf.writeByte(1)
                        val v: ByteArray = r.value
                        raf.writeInt(v.size)
                        raf.write(v)
                    }
                }
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

    // Read the index from disk and materialize it into memory
    internal fun buildIndex(): Map<String, Long> {
        raf.seek(0)
        val idx = HashMap<String, Long>()
        val indexStart = raf.readLong()
        raf.seek(indexStart)
        while (true) {
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
            val type = raf.readByte().toInt()
            when (type) {
                0 -> return null
                1 -> {
                    val len = raf.readInt()
                    val v = ByteArray(len)
                    raf.read(v)
                    return v
                }
                else -> throw Exception("Unexpected type tag: $type")
            }
        } else {
            return null
        }
    }
}
