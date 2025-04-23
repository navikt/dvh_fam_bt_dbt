{{
    config(
        materialized='table'
    )
}}

with barnetrygd_meta_data as (
select pk_bt_meta_data, kafka_offset, kafka_mottatt_dato, melding
from
(
  select meta.*
        ,json_value(melding, '$.behandlingsId') behandlingsId
        ,row_number() over (partition by json_value(melding, '$.behandlingsId') order by kafka_offset desc) nr
  from {{ source ('fam_bt', 'fam_bt_meta_data') }} meta
  where kafka_mottatt_dato >= sysdate - 30
)
where nr = 1
and kafka_offset not in (select kafka_offset from {{ source ('fam_bt', 'fam_bt_fagsak') }})
)

select * from barnetrygd_meta_data




