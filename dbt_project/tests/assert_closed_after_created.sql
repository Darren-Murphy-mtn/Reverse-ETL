-- An issue can't be closed before it was created. Returns offending rows (should be none).
select issue_id, created_at, closed_at
from {{ ref('stg_issues') }}
where closed_at is not null
  and closed_at < created_at
