with source as (
    select * from {{ source('github', 'raw_comments') }}
),

renamed as (
    select
        id as comment_id,
        _source_repo as repo_full_name,
        _source_repo || '#' || regexp_extract(issue_url, '/issues/(\d+)$', 1) as issue_id,
        cast(regexp_extract(issue_url, '/issues/(\d+)$', 1) as integer) as issue_number,
        coalesce("user".login, 'ghost') as author,
        coalesce("user".type = 'Bot', false) as author_is_bot,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        length(body) as body_length,
        contains(coalesce(body, ''), '{{ var("stale_marker") }}') as has_sync_marker
    from source
)

select *
from renamed
qualify row_number() over (partition by comment_id order by updated_at desc) = 1
