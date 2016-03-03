package com.andrewberls.sstable

sealed class Record {
    object Tombstone : Record()
    class Value(val value: ByteArray) : Record()
}
