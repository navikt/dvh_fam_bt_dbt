{{
    config(
        materialized='view'
    )
}}

with gruppe as (
    select -1 as alder_fra_og_med, -1 as alder_til_og_med, '00 I alt' as alder_gruppe_besk, 2 as alder_gruppe_nivaa from dual

    union
    select 0 as alder_fra_og_med, 20 as alder_til_og_med, '20 år og yngre' as alder_gruppe_besk, 1 as alder_gruppe_nivaa from dual

    union
    select 21 as alder_fra_og_med, 29 as alder_til_og_med, '21-29 år' as alder_gruppe_besk, 1 as alder_gruppe_nivaa from dual

    union
    select 30 as alder_fra_og_med, 39 as alder_til_og_med, '30-39 år' as alder_gruppe_besk, 1 as alder_gruppe_nivaa from dual

    union
    select 40 as alder_fra_og_med, 49 as alder_til_og_med, '40-49 år' as alder_gruppe_besk, 1 as alder_gruppe_nivaa from dual

    union
    select 50 as alder_fra_og_med, 59 as alder_til_og_med, '50-59 år' as alder_gruppe_besk, 1 as alder_gruppe_nivaa from dual

    union
    select 60 as alder_fra_og_med, 200 as alder_til_og_med, '60 år+' as alder_gruppe_besk, 1 as alder_gruppe_nivaa from dual
)

select
    alder_gruppe_besk
   ,alder_fra_og_med
   ,alder_til_og_med
   ,alder_gruppe_nivaa
from gruppe