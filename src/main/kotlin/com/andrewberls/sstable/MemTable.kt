package com.andrewberls.sstable

import java.util.concurrent.ConcurrentSkipListMap
import kotlin.collections.List
import com.andrewberls.sstable.Record

data class MemTable(
        private val capacity: Long,
        private val table: ConcurrentSkipListMap<String, Record> =
            ConcurrentSkipListMap<String, Record>()) {
    fun atCapacity(): Boolean =
        table.keys.size >= capacity

    fun entries(): List<Pair<String, Record>> =
        table.entries.map { e -> Pair(e.key, e.value) }

    fun get(k: String): ByteArray? {
        val r: Record? = table.get(k)
        if (r == null) {
            return null
        } else {
            when (r) {
                is Record.Tombstone -> return null
                is Record.Value -> return r.value
            }
        }
    }

    fun putRecord(k: String, r: Record): Unit {
        table.put(k, r)
    }
}
