select
    repo_full_name,
    run_id,
    cast(window_start as timestamp) as window_start,
    cast(extracted_at as timestamp) as snapshot_at,
    truncated as is_truncated
from {{ source('github', 'raw_extraction_runs') }}
