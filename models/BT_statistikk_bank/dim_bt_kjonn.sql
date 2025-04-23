{{
    config(
        materialized='view'
    )
}}

select distinct kjonn_kode, kjonn_flertall_besk as kjonn_besk, kjonn_nr
from {{ source('bt_statistikk_bank_dt_kodeverk', 'dim_kjonn') }}
where kjonn_flertall_besk in ('Kvinner','Menn')

union all
select 'ALT' as kjonn_kode, 'I alt' as kjonn_besk, -1 as kjonn_nr
from dual