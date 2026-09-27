select issue_id, created_at, first_response_at
from {{ ref('int_issue_lifecycle') }}
where first_response_at < created_at
