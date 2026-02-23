alter view recon_trn_vschedulerhistory as
select
  a.dataset_code,
  a.dataset_name,
  a.dataset_category,
  c.file_name as scheduler_file_name,
  e.*
from recon_mst_tdataset as a
inner join con_mst_tpipeline as b on a.dataset_code = b.target_dataset_code and b.delete_flag = 'N'
inner join con_trn_tscheduler as c on b.pipeline_code = c.pipeline_code and c.delete_flag = 'N'
inner join recon_trn_tscheduler as d on c.scheduler_gid = d.scheduler_gid and d.delete_flag = 'N'
inner join recon_trn_tjob as e on d.job_gid = e.job_gid and e.delete_flag = 'N'
where a.active_status = 'Y'
and a.delete_flag = 'N';