select dataset_name, rule_name, severity,
       sum(total_records) as records_checked,
       sum(failed_records) as records_failed,
       count(*) as evaluations,
       sum(case when status = 'failed' then 1 else 0 end) as failed_evaluations,
       max(checked_at) as last_checked_at
from {{ source('raw', 'quality_results') }}
group by dataset_name, rule_name, severity
