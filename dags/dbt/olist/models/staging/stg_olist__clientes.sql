with src as ( select * from {{ source('bronze','CUSTOMERS') }} )
select
    CUSTOMER_ID         as cliente_id,
    CUSTOMER_CITY       as cidade,
    CUSTOMER_STATE      as estado
from src
