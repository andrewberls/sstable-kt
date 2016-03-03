package com.andrewberls.sstable

import java.io.EOFException
import java.io.RandomAccessFile
import java.util.ArrayList
import java.util.HashMap
import kotlin.collections.List
import com.andrewberls.sstable.MarkerType
import com.andrewberls.sstable.Record
import com.andrewberls.sstable.Utils

/**
 * A set of key-value pairs stored in sorted order on disk, along with an index
 * of file pointer offsets for each key. Materializes the index into
 * memory when constructed.
 *
 * Header:
 *     index offset: long (8)
 * KVs...
 *     k length: int (4)
 *     k: bytes (x)
 *     type marker (value or deletion tombstone): byte (1)
 *     If not tombstone:
 *         v length: int (4)
 *         v: bytes (x)
 * Index
 *     k length: int (4)
 *     k: bytes (x)
 *     type marker offset: long (8)
 */
class DiskTable(val raf: RandomAccessFile) {
    companion object {
        // Invariant: kvs presorted in ascending order
        // TODO: check/enforce this
        fun build(kvs: List<Pair<String, Record>>): DiskTable {
            val raf = RandomAccessFile(Utils.tempfile(), "rws")
            val index = ArrayList<Pair<String, Long>>()

            // Make room for headers (index offset: Long)
            raf.seek(8)

            // Write KVs
            kvs.forEach { p ->
                val (k, r) = p
                when (r.type) {
                    MarkerType.DELETION -> {
                        Utils.writeLengthPrefixedString(raf, k)
                        index.add(Pair(k, raf.getFilePointer()))
                        raf.writeByte(r.type.repr)
                    }
                    MarkerType.VALUE -> {
                        Utils.writeLengthPrefixedString(raf, k)
                        index.add(Pair(k, raf.getFilePointer()))
                        raf.writeByte(r.type.repr)
                        val v: ByteArray = r.value!!
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
            val type = raf.readByte().toInt()
            when (MarkerType.fromRepr(type)) {
                MarkerType.DELETION -> return null
                MarkerType.VALUE -> {
                    val len = raf.readInt()
                    val v = ByteArray(len)
                    raf.read(v)
                    return v
                }
            }
        } else {
            return null
        }
    }
}
