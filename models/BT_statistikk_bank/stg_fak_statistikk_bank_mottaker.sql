{{
    config(
        materialized='table'
    )
}}

with geografi as (
    select
        pk_dim_geografi
       ,navarende_fylke_nr
       ,gtverdi
       ,gyldig_fra_dato
       ,gyldig_til_dato
       ,kommune_navn
       ,fylke_nr
    from {{ source('bt_statistikk_bank_dt_kodeverk', 'dim_geografi') }}
)
,

mottaker_periode as (
    select
        periode.aar
       ,periode.aar_kvartal
       ,periode.kvartal
       ,periode.kvartal_besk
       ,mottaker.*
       ,case when barn.fk_person1 is not null then 1 else 0 end barn_selv_mottaker_flagg
    from {{ source('bt_statistikk_bank_dvh_fam_bt', 'fam_bt_mottaker') }} mottaker

    left join
    (
        select stat_aarmnd, fk_person1
        from {{ source('bt_statistikk_bank_dvh_fam_bt', 'fam_bt_barn') }}
        where gyldig_flagg = 1
        and fk_person1 = fkb_person1 --Barn selv er mottaker
        group by stat_aarmnd, fk_person1
    ) barn
    on mottaker.fk_person1 = barn.fk_person1
    and mottaker.stat_aarmnd = barn.stat_aarmnd

    join {{ source('bt_statistikk_bank_dvh_fam_bt', 'dim_bt_periode') }} periode
    on mottaker.stat_aarmnd = to_char(periode.siste_dato_i_perioden, 'yyyymm') --Siste måned i kvartal

    where ((mottaker.statusk != 4 and mottaker.stat_aarmnd <= 202212) --Publisert statistikk(nav.no) til og med 2022, har filtrert vekk Institusjon(statusk=4).
            or mottaker.stat_aarmnd >= 202301 --Statistikk fra og med 2023, inkluderer Institusjon.
          )
    and mottaker.gyldig_flagg = 1
    and mottaker.belop > 0 -- Etterbetalinger telles ikke
)
,

--Legg til fylkenr basert på fremmednøkkel til geografi
mottaker_fylkenr as (
    select
        mottaker.*
       ,geografi.navarende_fylke_nr
       ,geografi.kommune_navn
       ,geografi.fylke_nr
    from mottaker_periode mottaker

    left join geografi
    on mottaker.fk_dim_geografi_bosted = geografi.pk_dim_geografi
)
--select * from mottaker_geografi where kommune_navn = 'Lunner';
,

--Hent ut fylkenr basert på gtverdi for de som har Ukjent nåværende fylkenr etter forrige steg
mottaker_ukjent_gtverdi as
(
    select
        mottaker_fylkenr.fk_person1
       ,mottaker_fylkenr.aar
       ,mottaker_fylkenr.aar_kvartal
       ,mottaker_fylkenr.kvartal
       ,mottaker_fylkenr.kvartal_besk
       ,mottaker_fylkenr.stat_aarmnd
       ,mottaker_fylkenr.barn_selv_mottaker_flagg
       ,mottaker_fylkenr.alder
       ,mottaker_fylkenr.kjonn
       ,mottaker_fylkenr.mottaker_gt_verdi
       ,dim_land.land_iso_3_kode
       ,case when dim_land.land_iso_3_kode is not null then '98'
             else gt_verdi.navarende_fylke_nr
        end navarende_fylke_nr --Når det er landskode på gtverdi, tilhører det Utland(fylkenr=98)
       ,gt_verdi.gtverdi
    from mottaker_fylkenr

    left outer join
    (
        select distinct land_iso_3_kode
        from dt_kodeverk.dim_land
    ) dim_land
    on mottaker_fylkenr.mottaker_gt_verdi = dim_land.land_iso_3_kode

    left join
    (
        select gtverdi
              ,max(navarende_fylke_nr) keep (dense_rank first order by gyldig_fra_dato desc) navarende_fylke_nr
        from geografi
        group by gtverdi
    ) gt_verdi
    on mottaker_fylkenr.mottaker_gt_verdi = gt_verdi.gtverdi

    where mottaker_fylkenr.navarende_fylke_nr = 'Ukjent'
)
--select * from mottaker_ukjent_gtverdi;
,

mottaker_fylkenr_alle as
(
    select
        fk_person1
       ,aar
       ,aar_kvartal
       ,kvartal
       ,kvartal_besk
       ,stat_aarmnd
       ,barn_selv_mottaker_flagg
       ,alder
       ,kjonn
       ,navarende_fylke_nr
    from mottaker_fylkenr
    where navarende_fylke_nr != 'Ukjent' or navarende_fylke_nr is null

    union all
    select
        fk_person1
       ,aar
       ,aar_kvartal
       ,kvartal
       ,kvartal_besk
       ,stat_aarmnd
       ,barn_selv_mottaker_flagg
       ,alder
       ,kjonn
       ,navarende_fylke_nr
    from mottaker_ukjent_gtverdi
)
--select * from mottaker_alle;
,

mottaker_navarende_fylke as (
  select
      mottaker.*
     ,fylke.nåværende_fylke_nr_navn
     ,fylke.nåværende_fylke_nr
     ,fylke.nåværende_fylkenavn
  from mottaker_fylkenr_alle mottaker

  left join {{ source('bt_statistikk_bank_dvh_fam_bt', 'dim_bt_navarende_fylke') }} fylke
  on fylke.nåværende_fylke_nr = coalesce(mottaker.navarende_fylke_nr, '99')
)
,

--Legg til kjønn og alder
navarende_fylke_kjonn_alder as (
    select
        mottaker.*
       ,kjonn.kjonn_besk
       ,alder_gruppe.alder_gruppe_besk

    from mottaker_navarende_fylke mottaker

    left join {{ source('bt_statistikk_bank_dvh_fam_bt', 'dim_bt_alder_gruppe') }} alder_gruppe
    on mottaker.alder between alder_gruppe.alder_fra_og_med and alder_gruppe.alder_til_og_med

    left join {{ source('bt_statistikk_bank_dvh_fam_bt', 'dim_bt_kjonn') }} kjonn
    on mottaker.kjonn = kjonn.kjonn_kode
)
select *
from navarende_fylke_kjonn_alder


--Last opp kun ny periode siden siste periode fra tabellen
--Tidligste periode fra tabellen er 201401
{% if is_incremental() %}

where stat_aarmnd > (select coalesce(max(stat_aarmnd), 201500) from {{ this }})

{% endif %}