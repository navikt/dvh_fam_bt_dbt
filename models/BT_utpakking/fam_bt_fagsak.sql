{{
    config(
        materialized='incremental'
    )
}}
with barnetrygd_meta_data as (
    select * from {{ref ('bt_meldinger_til_aa_pakke_ut')}}
),

bt_person as (
    select * from {{ ref ('fam_bt_person') }}
),

pre_final as (
    select *
    from barnetrygd_meta_data,
    json_table(melding, '$'
        columns (
            behandling_opprinnelse         varchar2(255) path '$.behandlingOpprinnelse'
           ,behandling_type                varchar2(255) path '$.behandlingTypeV2'
           ,fagsak_id                      varchar2(255) path '$.fagsakId'
           ,behandlings_id                 varchar2(255) path '$.behandlingsId'
           ,fagsak_type                    varchar2(255) path '$.fagsakType'
           ,tidspunkt_vedtak               varchar2(255) path '$.tidspunktVedtak'
           ,enslig_forsorger               varchar2(255) path '$.ensligForsørger'
           ,kategori                       varchar2(255) path '$.kategoriV2'
           ,funksjonell_id                 varchar2(255) path '$.funksjonellId'
           ,person_ident                   varchar2(255) path '$.personV2[*].personIdent'
           ,behandling_aarsak              varchar2(255) path '$.behandlingÅrsakV2'
           ,siste_iverksatte_behandlingsid NUMBER(38,0) path '$.sisteIverksatteBehandlingId'
        )
    ) j
),

final as (
    select
        p.behandling_opprinnelse
       ,p.behandling_type
       ,p.fagsak_id
       ,p.behandlings_id
       ,p.fagsak_type
       ,case
            when length(tidspunkt_vedtak) = 25 then cast(to_timestamp_tz(tidspunkt_vedtak, 'yyyy-mm-dd"T"hh24:mi:ss TZH:TZM') at time zone 'Europe/Belgrade' as timestamp)
            else cast(to_timestamp_tz(tidspunkt_vedtak, 'FXYYYY-MM-DD"T"HH24:MI:SS.FXFF3TZH:TZM') at time zone 'Europe/Belgrade' as timestamp)
        end tidspunkt_vedtak
       ,case
            when p.enslig_forsorger = 'false' then '0'
            else '1'
        end as enslig_forsorger
       ,p.kategori
       ,'BT' as kildesystem
       ,sysdate as lastet_dato
       ,p.funksjonell_id
       ,p.behandling_aarsak
       ,p.siste_iverksatte_behandlingsid
       ,p.person_ident
       ,p.kafka_offset
       ,p.kafka_mottatt_dato
       ,pk_bt_meta_data as fk_bt_meta_data
       ,per.pk_bt_person as fk_bt_person
    from pre_final p
    join bt_person per
    on p.kafka_offset = per.kafka_offset
    where per.soker_flagg = 1
)

select
    dvh_fambt_kafka.hibernate_sequence.nextval as pk_bt_fagsak
   ,fk_bt_person
   ,fk_bt_meta_data
   ,behandling_opprinnelse
   ,behandling_type
   ,fagsak_id
   ,funksjonell_id
   ,behandlings_id
   ,tidspunkt_vedtak
   ,enslig_forsorger
   ,kategori
   ,kafka_offset
   ,kildesystem
   ,lastet_dato
   ,behandling_aarsak
   ,fagsak_type
   ,kafka_mottatt_dato
   ,siste_iverksatte_behandlingsid
from final