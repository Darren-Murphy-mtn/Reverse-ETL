with source as (
    select * from {{ source('github', 'raw_issues') }}
),

renamed as (
    select
        _source_repo || '#' || cast(number as varchar) as issue_id,
        id as github_issue_id,
        _source_repo as repo_full_name,
        cast(number as integer) as issue_number,
        trim(title) as title,
        lower(state) as state,
        state_reason,
        coalesce("user".login, 'ghost') as author,
        coalesce("user".type = 'Bot', false) as author_is_bot,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        cast(closed_at as timestamp) as closed_at,
        pull_request is not null as is_pull_request,
        cast(comments as integer) as comment_count,
        html_url
    from source
)

-- The windowed and open-issue streams overlap, and pages can shift while paginating.
-- Keep the most recently updated copy of each issue.
select *
from renamed
qualify row_number() over (partition by issue_id order by updated_at desc) = 1
