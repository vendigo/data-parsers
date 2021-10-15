select *
from jira_tickets;

select assignee, count(*) as tasks, sum(story_points) as totalPoints
from jira_tickets
where assignee <> ''
  and status = 'Done'
  and parent_id = 0
group by assignee
order by totalPoints desc;

select assignee, avg(story_points)
from jira_tickets
where assignee <> ''
  and status = 'Done'
  and parent_id = 0
  and story_points <> 0
group by assignee;

select assignee, avg(avgs)
from (
         select assignee, sum(story_points) as avgs
         from jira_tickets
         where assignee <> ''
           and status = 'Done'
           and parent_id = 0
           and story_points <> 0
           and sprint <> ']'
         group by assignee, sprint
     ) as avgs_stat
group by assignee;

select sprint, sum(story_points) from jira_tickets
where assignee = 'dmmarchenko'
group by sprint;

select distinct(assignee)
from jira_tickets