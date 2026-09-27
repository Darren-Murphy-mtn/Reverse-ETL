{#
  One row per issue/PR: first response, reopen count, label history, and last human activity.

  "Activity" deliberately excludes bots and the pipeline's own writes (the stale-flag comment
  and the stale label). Otherwise every reverse-ETL sync would reset the clock it's measuring.
#}
{% set activity_events = ['closed', 'reopened', 'labeled', 'unlabeled', 'assigned', 'unassigned',
                          'renamed', 'milestoned', 'demilestoned', 'locked', 'unlocked'] %}

with issues as (
    select * from {{ ref('stg_issues') }}
),

comments as (
    select * from {{ ref('stg_comments') }}
),

events as (
    select * from {{ ref('stg_events') }}
),

runs as (
    select * from {{ ref('stg_extraction_runs') }}
),

comment_rollup as (
    select
        c.issue_id,
        min(c.created_at) filter (
            where not c.author_is_bot and not c.has_sync_marker and c.author <> i.author
        ) as first_response_at,
        max(c.created_at) filter (where not c.author_is_bot and not c.has_sync_marker) as last_human_comment_at,
        count(*) filter (where not c.author_is_bot and not c.has_sync_marker) as human_comment_count,
        bool_or(c.has_sync_marker) as has_stale_flag
    from comments as c
    inner join issues as i on c.issue_id = i.issue_id
    group by c.issue_id
),

event_rollup as (
    select
        issue_id,
        count(*) filter (where event_type = 'reopened') as reopen_count,
        string_agg(
            case event_type when 'labeled' then '+' || label_name else '-' || label_name end,
            ' > ' order by created_at
        ) filter (where event_type in ('labeled', 'unlabeled')) as label_history,
        max(created_at) filter (
            where event_type in ('{{ activity_events | join("', '") }}')
              and not actor_is_bot
              and coalesce(label_name, '') <> '{{ var("stale_label") }}'
        ) as last_human_event_at
    from events
    group by issue_id
),

joined as (
    select
        i.issue_id,
        i.repo_full_name,
        i.issue_number,
        i.title,
        i.state,
        i.author,
        i.author_is_bot,
        i.is_pull_request,
        i.html_url,
        i.created_at,
        i.closed_at,
        cr.first_response_at,
        coalesce(cr.human_comment_count, 0) as human_comment_count,
        coalesce(cr.has_stale_flag, false) as has_stale_flag,
        coalesce(er.reopen_count, 0) as reopen_count,
        er.label_history,
        greatest(
            i.created_at,
            coalesce(cr.last_human_comment_at, i.created_at),
            coalesce(er.last_human_event_at, i.created_at)
        ) as observed_last_activity_at,
        r.window_start,
        r.snapshot_at
    from issues as i
    inner join runs as r on i.repo_full_name = r.repo_full_name
    left join comment_rollup as cr on i.issue_id = cr.issue_id
    left join event_rollup as er on i.issue_id = er.issue_id
)

select
    * exclude (observed_last_activity_at),
    date_diff('second', created_at, first_response_at) / 3600.0 as hours_to_first_response,
    -- Comments and events are only extracted inside the window. If nothing was observed there,
    -- the true last activity is somewhere before window_start, so window_start is a lower
    -- bound on staleness rather than an exact value.
    greatest(observed_last_activity_at, window_start) as last_activity_at,
    observed_last_activity_at < window_start as last_activity_is_lower_bound
from joined
