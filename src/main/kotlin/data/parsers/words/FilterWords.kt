package data.parsers.words

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.transactions.transaction
import java.io.File

const val FILE_PATH = """C:\_files\dumps\words\words-all.txt"""
const val OUT_FILE_PATH = """C:\_files\dumps\words\words-all-out.txt"""

fun main() {
    val words = parseFile(
        File(FILE_PATH), listOf("noun", "v_naz"),
        listOf("bad", "vulg", "fname", "lname", "pname", "geo", "prop", "p", "slang", "ns", "abbr")
    )
    println("Found ${words.size} words")
    writeToFile(words)
    writeToDb(words)
}

private fun parseFile(
    file: File, includeTags: List<String>,
    excludeTags: List<String>,
    limit: Int = Int.MAX_VALUE
): List<String> {
    return CSVParser.parse(
        file, Charsets.UTF_8, CSVFormat.DEFAULT
            .withDelimiter(' ')
    ).asSequence()
        .map {
            Word(it.get(0), it.get(2).split(":").toSet())
        }
        .filter { it.tags.containsAll(includeTags) }
        .filter { it.tags.all { tag -> !excludeTags.contains(tag) } }
        .map { it.text }
        .take(limit)
        .distinct()
        .toList()
}

private fun writeToFile(words: List<String>) {
    val file = File(OUT_FILE_PATH)
    val writer = file.printWriter()
    words.forEach { writer.println(it) }
}

private fun writeToDb(words: List<String>) {
    Database.connect(
        "jdbc:postgresql://localhost:5432/postgres",
        driver = "org.postgresql.Driver", user = "postgres", password = "testpassword"
    )
    transaction {
        words.forEach { word ->
            WordsTable.insert {
                it[text] = word
            }
        }
    }
}

object WordsTable : Table("ua_words") {
    val text = varchar("text", 256)
}

data class Word(val text: String, val tags: Set<String>)