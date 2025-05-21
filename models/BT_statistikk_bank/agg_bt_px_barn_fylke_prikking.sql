{{
    config(
        materialized='view'
    )
}}

with barn as (
    select *
    from {{ source('bt_statistikk_bank_dvh_fam_bt', 'stg_fak_statistikk_bank_barn') }}
    where barn_selv_mottaker_flagg = 0 --Barn selv mottar barnetrygd telles ikke
)
,

--Returnere full liste med alle fylke og alle periode. Dette er PX format spesifikk.
full_liste as (
    select
        fylke.*
       ,periode.aar_kvartal
       ,periode.forste_dato_i_perioden
       ,periode.siste_dato_i_perioden
    from {{ source('bt_statistikk_bank_dvh_fam_bt','dim_bt_navarende_fylke') }} fylke

    full outer join {{ source('bt_statistikk_bank_dvh_fam_bt', 'dim_bt_periode') }} periode
    on 1 = 1
),

--Gruppere på nivå aar_kvartal og fylke
agg as (
    select
        full_liste.aar_kvartal
       ,full_liste.nåværende_fylke_nr_navn
       ,full_liste.nåværende_fylkenavn
       ,full_liste.fylke_nivaa
       ,count(distinct barn.fkb_person1) as antall
    from full_liste

    left outer join barn
    on full_liste.aar_kvartal = barn.aar_kvartal
    and (
            (full_liste.nåværende_fylkenavn != 'I alt'
              and full_liste.nåværende_fylke_nr_navn = barn.nåværende_fylke_nr_navn
            )
            or
            (full_liste.nåværende_fylkenavn = 'I alt'
            )
        )

    group by
        full_liste.aar_kvartal
       ,full_liste.nåværende_fylke_nr_navn
       ,full_liste.nåværende_fylkenavn
)
,

--Prikking: Round antall mindre enn 5 oppover til nærmeste tier
--Tester prikking
prikking as (
    select
        agg.aar_kvartal
       ,agg.nåværende_fylke_nr_navn
       ,agg.antall
       ,agg.fylke_nivaa
       ,round(agg.antall+5, -1) as antall_prikking

       --Nivå opp
       ,nivaa_opp.nåværende_fylke_nr_navn nåværende_fylke_nr_navn_nivaa_opp
       ,nivaa_opp.antall as antall_nivaa_opp
       ,nivaa_opp.fylke_nivaa fylke_nivaa_nivaa_opp
       ,round(nivaa_opp.antall+5, -1)as antall_prikking_nivaa_opp
    from agg

    join agg nivaa_opp
    on agg.aar_kvartal = nivaa_opp.aar_kvartal
    and agg.fylke_nivaa < nivaa_opp.fylke_nivaa

    where agg.antall between 1 and 4530
),

prikking_liste as
(
    select distinct
           aar_kvartal
          ,nåværende_fylke_nr_navn
          ,antall
          ,fylke_nivaa
          ,antall_prikking
    from prikking

    union all
    select distinct
           aar_kvartal
          ,nåværende_fylke_nr_navn_nivaa_opp
          ,antall_nivaa_opp
          ,fylke_nivaa_nivaa_opp
          ,antall_prikking_nivaa_opp
    from prikking
)
--select * from prikking_liste where aar_kvartal = 202501;

select
    substr(agg.aar_kvartal,1,4)||'K'||substr(agg.aar_kvartal,6,1) as aar_kvartal
   ,agg.nåværende_fylke_nr_navn
   ,coalesce(prikking_liste.antall_prikking, agg.antall) as antall
   ,agg.prosent
   ,localtimestamp as lastet_dato
from agg

left join prikking_liste
on agg.aar_kvartal = prikking_liste.aar_kvartal
and agg.nåværende_fylke_nr_navn = prikking_liste.nåværende_fylke_nr_navn

--Last opp kun ny periode siden siste periode fra tabellen
{% if is_incremental() %}

where aar_kvartal > (select coalesce(max(aar_kvartal), 201500) from {{ this }})

{% endif %}