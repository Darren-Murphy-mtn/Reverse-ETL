with source as (
    select * from {{ source('github', 'raw_events') }}
),

renamed as (
    select
        id as event_id,
        _source_repo as repo_full_name,
        _source_repo || '#' || cast(issue.number as varchar) as issue_id,
        cast(issue.number as integer) as issue_number,
        lower(event) as event_type,
        coalesce(actor.login, 'ghost') as actor,
        coalesce(actor.type = 'Bot', false) as actor_is_bot,
        cast(created_at as timestamp) as created_at,
        label.name as label_name
    from source
)

select *
from renamed
qualify row_number() over (partition by event_id order by created_at desc) = 1
