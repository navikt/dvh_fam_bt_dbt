{{
    config(
        materialized='view'
    )
}}

select aar, aar_kvartal, kvartal, kvartal_besk, forste_dato_i_perioden, siste_dato_i_perioden
from {{ source('bt_statistikk_bank_dt_kodeverk', 'dim_tid') }}
where gyldig_flagg = 1
and dim_nivaa = 4 --På kvartal nivå
and aar between 2015 and 2024 --Test