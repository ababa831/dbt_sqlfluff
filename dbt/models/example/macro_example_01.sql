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

{{ 
config(materialized='ephemeral')
}}

select "{{ since }}" as pre_defined_dt