package data.parsers.dotabuff

import org.http4k.client.ApacheClient
import org.http4k.core.Method
import org.http4k.core.Request
import org.jetbrains.exposed.dao.IntEntity
import org.jetbrains.exposed.dao.IntEntityClass
import org.jetbrains.exposed.dao.id.EntityID
import org.jetbrains.exposed.dao.id.IntIdTable
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.`java-time`.datetime
import org.jetbrains.exposed.sql.transactions.transaction
import org.jsoup.Jsoup
import org.jsoup.nodes.Element
import java.io.File
import java.time.LocalDateTime

const val ACC_ID = 253137085
const val OLD_ADD_ID = 93642829
const val MATCHES_URL = "https://www.dotabuff.com/players/${ACC_ID}/matches"
const val LATEST_MATCH_ID = 732793093

fun main() {
    loadPages(3 downTo 1)
}

private fun loadPages(range: IntProgression) {
    Database.connect(
        "jdbc:postgresql://localhost:5432/postgres",
        driver = "org.postgresql.Driver", user = "postgres", password = "testpassword"
    )

    //transaction {
        //SchemaUtils.drop(GameRecordsTable)
        //SchemaUtils.create(GameRecordsTable)
    //}

    range.forEach {
        val records = loadPage(it)
        println(records)
        saveRecordsToDb(records)
    }
}

fun saveRecordsToDb(records: List<GameRecord>) {
    transaction {
        records.forEach {
            GameRecordEntity.new {
                matchId = it.matchId
                hero = it.hero
                result = it.result
                ranked = it.ranked
                soloQueue = it.soloQueue
                duration = it.duration
                kills = it.kills
                deaths = it.deaths
                assists = it.assists
                date = it.date
            }
        }
    }
}

private fun loadPage(pageNumber: Int): List<GameRecord> {
    val gameRecords: List<GameRecord>
    val pageContent = loadPageContent(pageNumber)
    gameRecords = parsePage(pageContent)
    println("Loaded page $pageNumber - ${gameRecords.size} records")
    if (gameRecords.isEmpty()) {
        throw RuntimeException("Enable to load page: $pageNumber")
    }
    return gameRecords
}

private fun parsePage(pageContent: String): List<GameRecord> {
    val doc = Jsoup.parse(pageContent)
    val table = doc.select("div.content-inner > section > section > article > table")
    return table.select("tr").asIterable()
        .filterIndexed { index, _ ->
            index > 0
        }
        .map { parseLine(it) }
        .filter { it.matchId > LATEST_MATCH_ID }
}

private fun parseLine(row: Element): GameRecord {
    val matchId = row.select("td.cell-large > a[href]").attr("href").substring(10).toInt()
    val hero = row.select("img.image-hero").attr("title")
    val result = if (!row.select("a.lost").isEmpty()) {
        "Lost"
    } else if (!row.select("a.won").isEmpty()) {
        "Won"
    } else if (!row.select("a.abandoned").isEmpty()) {
        "Abandoned"
    } else {
        "Unknown"
    }
    val ranked = row.toString().contains("Ranked")
    val time = row.select("td")[5].text().split(":")
    val minutes = if (time.size == 2) {
        time[0].toInt() + 1
    } else {
        time[0].toInt() * 60 + time[1].toInt() + 1
    }
    val kda = row.select("td > span.kda-record").text()
    val (kills, deaths, assists) = kda.split("/").map { it.toInt() }
    val dateRaw = row.select("time").attr("datetime").split("+")[0]
    val dateTime = LocalDateTime.parse(dateRaw)

    return GameRecord(
        matchId = matchId,
        hero = hero,
        result = result,
        ranked = ranked,
        duration = minutes,
        kills = kills, deaths = deaths, assists = assists,
        date = dateTime
    )
}

data class GameRecord(
    val matchId: Int,
    val hero: String, val result: String,
    val ranked: Boolean = true,
    val soloQueue: Boolean = true,
    val duration: Int,
    val kills: Int, val deaths: Int, val assists: Int,
    val date: LocalDateTime
)

class GameRecordEntity(id: EntityID<Int>) : IntEntity(id) {
    companion object : IntEntityClass<GameRecordEntity>(GameRecordsTable)

    var matchId by GameRecordsTable.matchId
    var hero by GameRecordsTable.hero
    var result by GameRecordsTable.result
    var ranked by GameRecordsTable.ranked
    var soloQueue by GameRecordsTable.soloQueue
    var duration by GameRecordsTable.duration
    var kills by GameRecordsTable.kills
    var deaths by GameRecordsTable.deaths
    var assists by GameRecordsTable.assists
    var date by GameRecordsTable.date
}

object GameRecordsTable : IntIdTable("DOTA_GAME_RECORDS") {
    val matchId = integer("match_id")
    val hero = varchar("hero", length = 50)
    val result = varchar("result", length = 50)
    val ranked = bool("ranked")
    val soloQueue = bool("solo_queue")
    val duration = integer("duration")
    val kills = integer("kills")
    val deaths = integer("deaths")
    val assists = integer("assists")
    val date = datetime("date")
}

private fun loadPageFromResources(): String {
    val file = File(ClassLoader.getSystemResource("page2.html").toURI())
    return file.readText()
}

private fun loadPageContent(page: Int): String {
    val client = ApacheClient()
    val request = Request(Method.GET, MATCHES_URL)
        .header("User-Agent", "Google Chrome")
        .query("enhance", "overview")
        .query("page", page.toString())
    return client(request).toString()
}