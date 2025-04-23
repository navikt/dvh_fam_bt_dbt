{{
    config(
        materialized='view'
    )
}}

with alle_varianter as (
    select
        periode.aar_kvartal
       ,kjonn.kjonn_besk
       ,alder_gruppe.alder_gruppe_besk
       ,fylke.nåværende_fylke_nr_navn
    from {{ source('bt_statistikk_bank_dvh_fam_bt','dim_bt_periode') }} periode
    full outer join {{ source('bt_statistikk_bank_dvh_fam_bt','dim_bt_kjonn') }} kjonn
    on 1 = 1
    full outer join {{ source('bt_statistikk_bank_dvh_fam_bt','dim_bt_alder_gruppe') }} alder_gruppe
    on 1 = 1
    full outer join {{ source('bt_statistikk_bank_dvh_fam_bt','dim_bt_navarende_fylke') }} fylke
    on 1 = 1

    where periode.aar_kvartal in (202304, 202404) --Test
)
,

resultat as (
    select
        alle_varianter.nåværende_fylke_nr_navn as fylke
       ,substr(alle_varianter.aar_kvartal,1,4) || 'K' || substr(alle_varianter.aar_kvartal,6,1) as aar_kvartal
       ,alle_varianter.kjonn_besk as kjonn
       ,alle_varianter.alder_gruppe_besk as aldersgruppe
       ,'antall' as statistikkvariabel
       ,mottaker.antall as px_data
    from alle_varianter

    left join {{ ref('agg_statistikk_bank_kjonn_alder_fylke') }} mottaker
    on alle_varianter.aar_kvartal = mottaker.aar_kvartal
    and alle_varianter.kjonn_besk = mottaker.kjonn_besk
    and alle_varianter.alder_gruppe_besk = mottaker.alder_gruppe_besk
    and alle_varianter.nåværende_fylke_nr_navn = mottaker.nåværende_fylke_nr_navn
)
select
    aar_kvartal
   ,decode(fylke, '00 I alt', '00 I alt - fylke', fylke) fylke
   ,decode(kjonn, 'I alt', 'I alt - kjonn', kjonn) kjonn
   ,decode(aldersgruppe, '00 I alt', '00 I alt - aldersgruppe', aldersgruppe) aldersgruppe
   ,statistikkvariabel
   ,case when px_data <= 5 then null else px_data end as px_data --Håndtering av prikking
from resultat