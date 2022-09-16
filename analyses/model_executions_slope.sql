with invocations as (
  select * from {{ ref('FCT_DBT__INVOCATIONS') }}
)

,models as (
  select * from {{ ref('DIM_DBT__MODELS') }} 
)

,modelexecs as (
  select * from "TDUNLAP_SANDBOX_DBT_ARTIFACTS"."PROD_DBT_ARTIFACTS"."FCT_DBT__MODEL_EXECUTIONS" 
)


,prep as (
  select 
      me.name
      ,row_number() over (partition by me.name order by me.run_started_at) as runnumber
      ,me.total_node_runtime as node_runtime
  from modelexecs me
  left join models m on me.model_execution_id = m.model_execution_id
  left join invocations i on i.command_invocation_id = me.command_invocation_id
  where i.target_schema = 'prod' 
    and me.status = 'success'
  order by 1,2
)

,prep2 as (
  select 
      name
      ,runnumber
      ,avg(runnumber) over (partition by name) as runnumber_bar
      ,node_runtime
      ,avg(node_runtime) over (partition by name) as node_runtime_bar
  from prep
)

,slope as (
  select
      name,
      sum((runnumber-runnumber_bar)*(node_runtime-node_runtime_bar))/sum((runnumber-runnumber_bar)*(runnumber-runnumber_bar)) as slope
  from prep2
  group by name
)

select 
    slope.name
    ,runnumber
    ,node_runtime
    ,slope
from prep2
join slope on prep2.name = slope.name
order by slope desc, name, runnumber

/*
select 
    m.name
    --,avg(case when me.status = 'success' then me.total_node_runtime else null end) as avg_successful_runtime
    ,SUM(me.total_node_runtime) as avg_successful_runtime
    ,count(distinct case when me.status = 'success' then me.model_execution_id else null end) as successful_run_count
    ,count(distinct case when me.status <> 'success' then me.model_execution_id else null end) as unsuccessful_run_count
from modelexecs me
left join models m on me.node_id = m.node_id
group by m.name
order by 2 desc
*/

 