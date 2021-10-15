package data.parsers

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.transactions.transaction
import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter

const val DIR = """C:\_files\dumps\jira"""
val FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern("dd/MMM/yy")

fun main() {
    val tickets = parseAllTickets()
    writeToDb(tickets)
}

fun writeToDb(tickets: List<JiraTicket>) {
    Database.connect(
        "jdbc:postgresql://localhost:5432/postgres",
        driver = "org.postgresql.Driver", user = "postgres", password = "testpassword"
    )
    transaction {
        println("Recreating tables")
        SchemaUtils.drop(JiraTickets)
        SchemaUtils.create(JiraTickets)

        tickets.forEachWithProgress { ticket ->
            JiraTickets.insert {
                it[issueId] = ticket.issueId
                it[key] = ticket.key
                it[parentId] = ticket.parentId
                it[summary] = ticket.summary
                it[assignee] = ticket.assignee
                it[sprint] = ticket.sprint
                it[sprintNum] = ticket.sprintNum
                it[storyPoints] = ticket.storyPoints
                it[status] = ticket.status
                it[updatedDate] = ticket.updatedDate
            }
        }
    }
}

object JiraTickets : Table("JIRA_TICKETS") {
    val issueId = long("issue_id")
    val key = varchar("key", length = 50)
    val parentId = long("parent_id").nullable()
    val summary = varchar("summary", length = 255).nullable()
    val assignee = varchar("assignee", length = 50).nullable()
    val sprint = varchar("sprint", length = 50).nullable()
    val sprintNum = integer("sprint_num").nullable()
    val storyPoints = double("story_points").nullable()
    val status = varchar("status", 50)
    val updatedDate = varchar("updatedDate", 50).nullable()

    override val primaryKey = PrimaryKey(issueId, name = "PK_ISSUE_ID") // name is optional here
}

data class JiraTicket(
    val key: String, val issueId: Long, val parentId: Long?, val summary: String,
    val assignee: String?, val sprint: String?, val sprintNum: Int?, val storyPoints: Double?, val updatedDate: String?,
    val status: String
)

private fun parseAllTickets() = File(DIR).walk()
    .filter { it.isFile }
    .map {
        parseFile(it)
    }
    .flatMap { it }
    .toList()

private fun parseFile(file: File): List<JiraTicket> {
    return CSVParser.parse(
        file, Charsets.UTF_8, CSVFormat.DEFAULT
            .withFirstRecordAsHeader()
            .withIgnoreHeaderCase()
            .withTrim()
    )
        .map {
            val sprint = it.get(5).ifBlank { null }

            JiraTicket(
                key = it.get("Issue key"),
                issueId = it.get("Issue id").toLong(),
                parentId = it.get("Parent id").safeToDouble()?.toLong(),
                summary = it.get("Summary"),
                assignee = it.get("Assignee").ifBlank { null },
                sprint = sprint,
                sprintNum = parseSprintNum(sprint),
                storyPoints = it.get("Custom field (Story Points)").safeToDouble(),
                updatedDate = parseDate(it.get("Updated")).toString(),
                status = it.get("Status")
            )
        }
}

fun parseSprintNum(sprint: String?): Int? {
    if (sprint == null || !sprint.startsWith("TTKG2 Sprint ")) {
        return null
    }
    return sprint.substring(13).toInt()
}

fun parseDate(d: String): LocalDate? {
    return if (d.isBlank()) {
        null
    } else {
        LocalDate.parse(d.substring(0, 9), FORMATTER)
    }
}

fun String.safeToDouble(): Double? {
    return if (this.isBlank()) {
        null
    } else {
        this.toDouble()
    }
}

inline fun <T> List<T>.forEachWithProgress(action: (T) -> Unit) {
    val onePercent = this.size / 100
    for ((i, element) in this.withIndex()) {
        action(element)
        if (i % onePercent == 0) {
            val processed = ((i.toDouble() / this.size) * 100).toInt()
            println("Processed $processed %")
        }
    }
}