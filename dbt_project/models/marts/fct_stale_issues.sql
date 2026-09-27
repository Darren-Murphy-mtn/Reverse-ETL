{# Read by reverse_etl/sync_stale_issues.py. Open issues (not PRs) past their repo's threshold. #}
with lifecycle as (
    select
        *,
        date_diff('day', last_activity_at, snapshot_at) as days_since_last_activity,
        {{ stale_threshold_days('repo_full_name') }} as stale_threshold_days
    from {{ ref('int_issue_lifecycle') }}
    where state = 'open' and not is_pull_request
)

select
    issue_id,
    repo_full_name,
    issue_number,
    title,
    author,
    html_url,
    created_at,
    last_activity_at,
    last_activity_is_lower_bound,
    days_since_last_activity,
    stale_threshold_days,
    has_stale_flag,
    snapshot_at
from lifecycle
where days_since_last_activity >= stale_threshold_days
