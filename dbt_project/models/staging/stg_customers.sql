select customer_id, lower(email) as email, country, segment, signup_ts
from {{ source('raw', 'customers') }}
