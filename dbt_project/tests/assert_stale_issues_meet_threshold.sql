-- Guards the reverse-ETL input: nothing below its threshold, no PRs, no closed issues.
select s.issue_id
from {{ ref('fct_stale_issues') }} as s
inner join {{ ref('stg_issues') }} as i on s.issue_id = i.issue_id
where s.days_since_last_activity < s.stale_threshold_days
   or i.state <> 'open'
   or i.is_pull_request
