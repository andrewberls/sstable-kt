package com.andrewberls.sstable

import java.io.File
import java.io.RandomAccessFile

object Utils {
    fun tempfile(): File {
        val f = File.createTempFile("sstable", ".tmp")
        f.deleteOnExit()
        return f
    }

    /**
     * Read a length-prefixed (4-byte int) String from a RandomAccessFile
     * Mutates file pointer of `raf`!
     */
    fun readLengthPrefixedString(raf: RandomAccessFile): String {
        val len = raf.readInt()
        return (0..len-1).map { raf.readByte().toChar() }.joinToString("")
    }

    /**
     * Write a length-prefixed (4-byte int) String to a RandomAccessFile
     * Mutates file pointer of `raf`!
     */
    fun writeLengthPrefixedString(raf: RandomAccessFile, str: String): Unit {
        raf.writeInt(str.length)
        raf.writeBytes(str)
    }
}
