create view con_trn_vscheduler as
select
  a.scheduler_gid,
  a.scheduled_date,
  a.pipeline_code,
  b.pipeline_name,
  a.dataset_code,
  c.dataset_name,
  a.scheduler_parameters,
  a.file_name,
  a.scheduler_start_date,
  a.scheduler_end_date,
  a.scheduler_status,
  a.valid_record_count,
  a.error_record_count,
  a.scheduler_initiated_by,
  a.last_update_date
from con_trn_tscheduler as a
left join con_mst_tpipeline as b on a.pipeline_code = b.pipeline_code
  and b.delete_flag = 'N'
left join recon_mst_tdataset as c on a.dataset_code = c.dataset_code
  and c.delete_flag = 'N'
where a.delete_flag = 'N'