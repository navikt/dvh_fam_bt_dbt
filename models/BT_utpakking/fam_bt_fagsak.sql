{{
    config(
        materialized='incremental'
    )
}}
with barnetrygd_meta_data as (
  select * from {{ref ('bt_meldinger_til_aa_pakke_ut')}}
),

bt_person AS (
  SELECT * FROM {{ ref ('fam_bt_person') }}
),

pre_final as (
select * from barnetrygd_meta_data,
  json_table(melding, '$'
    COLUMNS (
          behandling_opprinnelse    VARCHAR2 PATH '$.behandlingOpprinnelse'
         ,behandling_type           VARCHAR2 PATH '$.behandlingTypeV2'
         ,fagsak_id                 VARCHAR2 PATH '$.fagsakId'
         ,behandlings_id            VARCHAR2 PATH '$.behandlingsId'
         ,fagsak_type               VARCHAR2 PATH '$.fagsakType'
         ,tidspunkt_vedtak          VARCHAR2 PATH '$.tidspunktVedtak'
         ,enslig_forsørger          VARCHAR2 PATH '$.ensligForsørger'
         ,kategori                  VARCHAR2 PATH '$.kategoriV2'
         ,underkategori             VARCHAR2 PATH '$.underkategoriV2'
         ,funksjonell_id            VARCHAR2 PATH '$.funksjonellId'
         ,person_ident              VARCHAR2 PATH '$.personV2[*].personIdent'
         ,behandling_Årsak           VARCHAR2 PATH '$.behandlingÅrsakV2'
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
    ,CASE
      WHEN LENGTH(tidspunkt_vedtak) = 25 THEN CAST(to_timestamp_tz(tidspunkt_vedtak, 'yyyy-mm-dd"T"hh24:mi:ss TZH:TZM') AT TIME ZONE 'Europe/Belgrade' AS TIMESTAMP)
      ELSE CAST(to_timestamp_tz(tidspunkt_vedtak, 'FXYYYY-MM-DD"T"HH24:MI:SS.FXFF3TZH:TZM') AT TIME ZONE 'Europe/Belgrade' AS TIMESTAMP)
      END tidspunkt_vedtak
    ,CASE
      WHEN p.enslig_forsørger = 'false' THEN '0'
      ELSE '1'
      END AS enslig_forsørger
    ,p.kategori
    ,p.underkategori
    ,'BT' AS kildesystem
    ,sysdate AS lastet_dato
    ,p.funksjonell_id
    ,p.behandling_Årsak
    ,p.person_ident
    ,p.kafka_offset
    ,p.kafka_mottatt_dato
    ,pk_bt_meta_data as FK_BT_META_DATA
    ,per.pk_bt_person as FK_BT_PERSON
  from pre_final p
  join bt_person per
  on p.kafka_offset = per.kafka_offset
  where per.soker_flagg = 1
)

select
    dvh_fambt_kafka.hibernate_sequence.nextval as PK_BT_FAGSAK
    ,FK_BT_PERSON
    ,FK_BT_META_DATA
    ,BEHANDLING_OPPRINNELSE
    ,BEHANDLING_TYPE
    ,FAGSAK_ID
    ,FUNKSJONELL_ID
    ,BEHANDLINGS_ID
    ,TIDSPUNKT_VEDTAK
    ,ENSLIG_FORSØRGER
    ,KATEGORI
    ,UNDERKATEGORI
    ,KAFKA_OFFSET
    ,KILDESYSTEM
    ,LASTET_DATO
    ,BEHANDLING_ÅRSAK
    ,FAGSAK_TYPE
    ,kafka_mottatt_dato
from final

