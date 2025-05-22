{{
    config(
        materialized='table'
    )
}}

with mottaker as (
    select *
    from {{ source('bt_statistikk_bank_dvh_fam_bt','stg_fak_statistikk_bank_mottaker') }}
    where barn_selv_mottaker_flagg = 0 --Barn selv mottar barnetrygd telles ikke
    and statusk in (2,3) --Utvidet barnetrygd
)
,

--Returnere en full liste med alle kjønn, aldersgrupper og alle perioder. Dette er PX format spesifikk.
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
       --,count(distinct fk_person1) as antall
       ,case when count(distinct fk_person1) < 10 then
                  round(count(distinct fk_person1)+5, -1) --Prikking: Round antall mindre enn 5 oppover til nærmeste tier
             else count(distinct fk_person1)
        end antall
    from full_liste

    left outer join mottaker
    on full_liste.aar_kvartal = mottaker.aar_kvartal
    and full_liste.kjonn_besk = mottaker.kjonn_besk
    and full_liste.alder_gruppe_besk = mottaker.alder_gruppe_besk

    where full_liste.kjonn_besk != 'I alt'
    and full_liste.alder_gruppe_besk != '00 I alt'

    group by
        full_liste.aar_kvartal
       ,full_liste.kjonn_besk
       ,full_liste.kjonn_nivaa
       ,full_liste.alder_gruppe_besk
       ,full_liste.alder_gruppe_nivaa
)
,

--kjonn_besk = 'I alt'
kjonn_i_alt as (
    select
        full_liste.aar_kvartal
       ,full_liste.kjonn_besk
       ,full_liste.kjonn_nivaa
       ,full_liste.alder_gruppe_besk
       ,full_liste.alder_gruppe_nivaa
       ,sum(agg.antall) as antall
    from full_liste

    left outer join agg
    on full_liste.aar_kvartal = agg.aar_kvartal
    and full_liste.alder_gruppe_besk = agg.alder_gruppe_besk

    where full_liste.kjonn_besk = 'I alt'
    and full_liste.alder_gruppe_besk != '00 I alt'

    group by
        full_liste.aar_kvartal
       ,full_liste.kjonn_besk
       ,full_liste.kjonn_nivaa
       ,full_liste.alder_gruppe_besk
       ,full_liste.alder_gruppe_nivaa
)
,

--alder_gruppe_besk = '00 I alt'
aldersgruppe_i_alt as (
    select
        full_liste.aar_kvartal
       ,full_liste.kjonn_besk
       ,full_liste.kjonn_nivaa
       ,full_liste.alder_gruppe_besk
       ,full_liste.alder_gruppe_nivaa
       ,sum(agg.antall) as antall
    from full_liste

    left outer join agg
    on full_liste.aar_kvartal = agg.aar_kvartal
    and full_liste.kjonn_besk = agg.kjonn_besk

    where full_liste.alder_gruppe_besk = '00 I alt'
    and full_liste.kjonn_besk != 'I alt'

    group by
        full_liste.aar_kvartal
       ,full_liste.kjonn_besk
       ,full_liste.kjonn_nivaa
       ,full_liste.alder_gruppe_besk
       ,full_liste.alder_gruppe_nivaa
)
,

--kjonn_besk = 'I alt' og alder_gruppe_besk = '00 I alt'
i_alt as (
    select
        full_liste.aar_kvartal
       ,full_liste.kjonn_besk
       ,full_liste.kjonn_nivaa
       ,full_liste.alder_gruppe_besk
       ,full_liste.alder_gruppe_nivaa
       ,sum(agg.antall) as antall
    from full_liste

    left outer join agg
    on full_liste.aar_kvartal = agg.aar_kvartal

    where full_liste.alder_gruppe_besk = '00 I alt'
    and full_liste.kjonn_besk = 'I alt'

    group by
        full_liste.aar_kvartal
       ,full_liste.kjonn_besk
       ,full_liste.kjonn_nivaa
       ,full_liste.alder_gruppe_besk
       ,full_liste.alder_gruppe_nivaa
)
,

alle as (
    select
        agg.aar_kvartal
       ,agg.kjonn_besk
       ,agg.kjonn_nivaa
       ,agg.alder_gruppe_besk
       ,agg.alder_gruppe_nivaa
       ,agg.antall
       ,case when aldersgruppe_i_alt.antall != 0 then round(agg.antall/aldersgruppe_i_alt.antall*100,1) else 0 end prosent
    from agg
    join aldersgruppe_i_alt
    on agg.aar_kvartal = aldersgruppe_i_alt.aar_kvartal
    and agg.kjonn_besk = aldersgruppe_i_alt.kjonn_besk

    union all
    select
        kjonn_i_alt.aar_kvartal
       ,kjonn_i_alt.kjonn_besk
       ,kjonn_i_alt.kjonn_nivaa
       ,kjonn_i_alt.alder_gruppe_besk
       ,kjonn_i_alt.alder_gruppe_nivaa
       ,kjonn_i_alt.antall
       ,case when i_alt.antall != 0 then round(kjonn_i_alt.antall/i_alt.antall*100,1) else 0 end prosent
    from kjonn_i_alt
    join i_alt
    on kjonn_i_alt.aar_kvartal = i_alt.aar_kvartal

    union all
    select
        aar_kvartal
       ,kjonn_besk
       ,kjonn_nivaa
       ,alder_gruppe_besk
       ,alder_gruppe_nivaa
       ,antall
       ,100 as prosent
    from aldersgruppe_i_alt

    union all
    select
        aar_kvartal
       ,kjonn_besk
       ,kjonn_nivaa
       ,alder_gruppe_besk
       ,alder_gruppe_nivaa
       ,antall
       ,100 as prosent
    from i_alt
)
select
    substr(aar_kvartal,1,4)||'K'||substr(aar_kvartal,6,1) as aar_kvartal
   ,kjonn_besk as kjonn
   ,alder_gruppe_besk as aldersgruppe
   ,antall
   ,prosent
   ,localtimestamp as lastet_dato
from alle