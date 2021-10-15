select year, month, sum(duration) as totalDuration,
       count(*) as totalGames
from (
         select extract(YEARS FROM date)  as year,
                extract(MONTHS FROM date) as month,
                hero,
                result,
                duration
         from dota_game_records) month_games
group by year, month
order by year desc, month desc;