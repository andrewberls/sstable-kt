package com.andrewberls.sstable

import java.util.concurrent.ConcurrentSkipListMap
import kotlin.collections.List

class MemTable {
    private val table = ConcurrentSkipListMap<String, ByteArray>()

    fun entries(): List<Pair<String, ByteArray>> =
        table.entries.map { e -> Pair(e.key, e.value) }

    fun get(k: String): ByteArray? =
        table.get(k)

    fun put(k: String, v: ByteArray): Unit {
        table.put(k, v)
    }
}
