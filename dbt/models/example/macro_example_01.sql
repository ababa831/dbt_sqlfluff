-- dataformの pre_operations をdbt用のプラクティスに基づいて書き替えたもの
-- TODO: UDFとしてmacrosに切り出して、認識できるようにする。（切り出した際の挙動が不自然なので保留中）
{%- if is_incremental() -%}
{%- call statement('time_checkpoint', fetch_result=True)  -%}
select max(time) from {{ this }}
{%- endcall -%}
{%- else -%}
{%- call statement('time_checkpoint', fetch_result=True)  -%}
select timestamp('2023-02-19')
{%- endcall -%}
{%- endif -%}
{%- set time_checkpoint = load_result('time_checkpoint') -%}
{%- set time_checkpoint_val = time_checkpoint['data'][0][0] -%}
{%- call statement('since', fetch_result=True)  -%}
SELECT DATE_SUB(DATE("{{ time_checkpoint_val }}"), INTERVAL 3 DAY)
{%- endcall -%}
{%- set since_result = load_result('since') -%}
{%- set since = since_result['data'][0][0] -%}


-- TEMPORARY UDF (TODO: macrosディレクトリから認識できるようにしたい)
{% call set_sql_header(config) %}
CREATE TEMPORARY FUNCTION ua_to_version(ua STRING) 
RETURNS STRING LANGUAGE js OPTIONS (library = "gs://bigquery_assets/udf/woothee.js")
AS """return woothee.parse(ua).os_version;""";
{%- endcall %}

{{ 
config(materialized='ephemeral')
}}

select "{{ since }}" as pre_defined_dt