{{
    config(
        materialized='table'
    )
}}

with mottaker as (
    select *
    from {{ source('bt_statistikk_bank_dvh_fam_bt','stg_fak_statistikk_bank_mottaker') }}
    where barn_selv_mottaker_flagg = 0 --Barn selv mottar barnetrygd telles ikke
)
,

--Returnere en full liste med alle kjønn, aldersgruppe og alle periode. Dette er PX format spesifikk.
full_liste as (
    select
        kjonn.kjonn_besk
       ,kjonn.kjonn_nivaa
       ,alder_gruppe.alder_gruppe_besk
       ,alder_gruppe.alder_gruppe_nivaa
       ,periode.aar_kvartal
       ,periode.forste_dato_i_perioden
       ,periode.siste_dato_i_perioden
    from {{ source('bt_statistikk_bank_dvh_fam_bt','dim_bt_kjonn') }} kjonn

    full outer join {{ source('bt_statistikk_bank_dvh_fam_bt','dim_bt_alder_gruppe') }} alder_gruppe
    on 1 = 1

    full outer join {{ source('bt_statistikk_bank_dvh_fam_bt', 'dim_bt_periode') }} periode
    on 1 = 1
)
,

--Gruppere på nivå aar_kvartal, kjønn og aldersgruppe.
agg as (
    select
        full_liste.aar_kvartal
       ,full_liste.kjonn_besk
       ,full_liste.kjonn_nivaa
       ,full_liste.alder_gruppe_besk
       ,full_liste.alder_gruppe_nivaa
       ,count(distinct fk_person1) as antall
    from full_liste

    left outer join mottaker
    on full_liste.aar_kvartal = mottaker.aar_kvartal
    and (
         (full_liste.kjonn_besk != 'I alt'
          and full_liste.alder_gruppe_besk != '00 I alt'
          and full_liste.kjonn_besk = mottaker.kjonn_besk
          and full_liste.alder_gruppe_besk = mottaker.alder_gruppe_besk
         )
         or
         (full_liste.kjonn_besk = 'I alt'
          and full_liste.alder_gruppe_besk != '00 I alt'
          and full_liste.alder_gruppe_besk = mottaker.alder_gruppe_besk
         )
         or
         (full_liste.alder_gruppe_besk = '00 I alt'
          and full_liste.kjonn_besk != 'I alt'
          and full_liste.kjonn_besk = mottaker.kjonn_besk
         )
         or
         (full_liste.alder_gruppe_besk = '00 I alt'
          and full_liste.kjonn_besk = 'I alt'
         )
        )

    group by
        full_liste.aar_kvartal
       ,full_liste.kjonn_besk
       ,full_liste.kjonn_nivaa
       ,full_liste.alder_gruppe_besk
       ,full_liste.alder_gruppe_nivaa
)
,

--Prikking: Round antall mindre enn 5 oppover til nærmeste tier
--Tester prikking
prikking as (
    select
        agg.*
       ,round(agg.antall+5, -1) as antall_prikking

       --Nivå opp
       ,nivaa_opp.kjonn_besk as kjonn_besk_nivaa_opp
       ,nivaa_opp.alder_gruppe_besk as alder_gruppe_besk_nivaa_opp
       ,nivaa_opp.antall as antall_nivaa_opp
       ,nivaa_opp.kjonn_nivaa as kjonn_nivaa_nivaa_opp
       ,nivaa_opp.alder_gruppe_nivaa as alder_gruppe_nivaa_nivaa_opp
       ,round(nivaa_opp.antall+5, -1)as antall_prikking_nivaa_opp
    from agg

    join agg nivaa_opp
    on agg.aar_kvartal = nivaa_opp.aar_kvartal
    and (agg.alder_gruppe_nivaa < nivaa_opp.alder_gruppe_nivaa
        or agg.kjonn_nivaa < nivaa_opp.kjonn_nivaa)

    where agg.antall between 1 and 6
),
prikking_liste as
(
    select distinct
           aar_kvartal
          ,kjonn_besk
          ,kjonn_nivaa
          ,alder_gruppe_besk
          ,alder_gruppe_nivaa
          ,antall
          ,antall_prikking
    from prikking

    union all
    select distinct
           aar_kvartal
          ,kjonn_besk_nivaa_opp
          ,kjonn_nivaa_nivaa_opp
          ,alder_gruppe_besk_nivaa_opp
          ,alder_gruppe_nivaa_nivaa_opp
          ,antall_nivaa_opp
          ,antall_prikking_nivaa_opp
    from prikking
)
--select * from prikking_liste where aar_kvartal = 202501;

select
    substr(agg.aar_kvartal,1,4)||'K'||substr(agg.aar_kvartal,6,1) as aar_kvartal
   ,agg.kjonn_besk as kjonn
   ,agg.alder_gruppe_besk as aldersgruppe
   ,coalesce(prikking_liste.antall_prikking, agg.antall) as antall
   ,localtimestamp as lastet_dato
from agg

left join prikking_liste
on agg.aar_kvartal = prikking_liste.aar_kvartal
and agg.kjonn_besk = prikking_liste.kjonn_besk
and agg.alder_gruppe_besk = prikking_liste.alder_gruppe_besk