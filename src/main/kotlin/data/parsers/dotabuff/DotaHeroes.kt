package data.parsers.dotabuff

import org.http4k.client.ApacheClient
import org.http4k.core.Method
import org.http4k.core.Request
import org.jetbrains.exposed.dao.id.IntIdTable
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.transactions.transaction
import org.jsoup.Jsoup
import org.jsoup.nodes.Element

const val HEROES_URL = "https://www.dotabuff.com/heroes"

fun main() {
    val heroes = loadHeroes()
    saveHeroes(heroes)
    heroes.forEach { println(it) }
}

fun loadHeroes(): List<Hero> {
    val pageContent = loadPageContent()
    val doc = Jsoup.parse(pageContent)
    val heroes = doc.select("div.hero-grid")[0].childNodes().toMutableList()
    heroes.removeLast()

    return heroes.map { hero ->
        val heroName = (hero as Element).select("div.name").text()
        val link = hero.attr("href").substring(8)
        Hero(heroName, link)
    }
}

fun saveHeroes(heroes: List<Hero>) {
    Database.connect(
        "jdbc:postgresql://localhost:5432/postgres",
        driver = "org.postgresql.Driver", user = "postgres", password = "testpassword"
    )
    transaction {
        SchemaUtils.drop(DotaHeroes)
        SchemaUtils.create(DotaHeroes)

        heroes.forEach { hero ->
            DotaHeroes.insert {
                it[name] = hero.name
                it[link] = hero.link
            }
        }
    }
}

fun loadPageContent(): String {
    val client = ApacheClient()
    val request = Request(Method.GET, HEROES_URL)
        .header("User-Agent", "Google Chrome")
    return client(request).toString()
}

data class Hero(val name: String, val link: String)

object DotaHeroes : IntIdTable("DOTA_HEROES") {
    val name = varchar("name", length = 50)
    val link = varchar("link", length = 50)
}