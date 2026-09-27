with lifecycle as (
    select * from {{ ref('int_issue_lifecycle') }}
),

closed as (
    select
        *,
        date_diff('second', created_at, closed_at) / 3600.0 as resolution_hours
    from lifecycle
    where state = 'closed'
      and closed_at is not null
      and closed_at >= window_start
)

select
    issue_id,
    repo_full_name,
    issue_number,
    is_pull_request,
    author,
    created_at,
    closed_at,
    round(resolution_hours, 2) as resolution_hours,
    {{ resolution_bucket('resolution_hours') }} as resolution_bucket,
    round(hours_to_first_response, 2) as hours_to_first_response,
    reopen_count
from closed
