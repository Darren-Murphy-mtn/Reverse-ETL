with runs as (
    select * from {{ ref('stg_extraction_runs') }}
),

opened as (
    select i.repo_full_name, i.author as contributor, i.author_is_bot as is_bot,
           date_trunc('week', i.created_at) as activity_week,
           1 as issues_opened, 0 as issues_closed, 0 as comments_posted
    from {{ ref('stg_issues') }} as i
    inner join runs as r on i.repo_full_name = r.repo_full_name
    where i.created_at >= r.window_start
),

closed as (
    select e.repo_full_name, e.actor, e.actor_is_bot,
           date_trunc('week', e.created_at),
           0, 1, 0
    from {{ ref('stg_events') }} as e
    inner join runs as r on e.repo_full_name = r.repo_full_name
    where e.event_type = 'closed' and e.created_at >= r.window_start
),

commented as (
    select c.repo_full_name, c.author, c.author_is_bot,
           date_trunc('week', c.created_at),
           0, 0, 1
    from {{ ref('stg_comments') }} as c
    inner join runs as r on c.repo_full_name = r.repo_full_name
    where c.created_at >= r.window_start and not c.has_sync_marker
),

unioned as (
    select * from opened
    union all
    select * from closed
    union all
    select * from commented
)

select
    repo_full_name || '|' || contributor || '|' || strftime(activity_week, '%Y-%m-%d') as contributor_week_id,
    repo_full_name,
    contributor,
    bool_or(is_bot) as is_bot,
    cast(activity_week as date) as activity_week,
    sum(issues_opened) as issues_opened,
    sum(issues_closed) as issues_closed,
    sum(comments_posted) as comments_posted
from unioned
group by repo_full_name, contributor, activity_week
