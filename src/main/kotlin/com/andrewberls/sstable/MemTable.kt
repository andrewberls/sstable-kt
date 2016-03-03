package com.andrewberls.sstable

import java.util.concurrent.ConcurrentSkipListMap
import kotlin.collections.List
import com.andrewberls.sstable.MarkerType
import com.andrewberls.sstable.Record

class MemTable(private val capacity: Long) {
    private var table = ConcurrentSkipListMap<String, Record>()

    fun atCapacity(): Boolean =
        table.keys.size >= capacity

    fun entries(): List<Pair<String, Record>> =
        table.entries.map { e -> Pair(e.key, e.value) }

    fun get(k: String): ByteArray? =
        table.get(k)?.value

    fun putRecord(k: String, r: Record): Unit {
        table.put(k, r)
    }
}
