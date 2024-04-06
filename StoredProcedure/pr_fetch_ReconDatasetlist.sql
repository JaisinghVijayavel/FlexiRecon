DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_fetch_ReconDatasetlist` $$
CREATE PROCEDURE `pr_fetch_ReconDatasetlist`
(
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32)
)
begin

  set @row_number := 0;

  select
    (@row_number := @row_number + 1) as sl_no,
    a.dataset_gid,
    a.dataset_code,
    a.dataset_name,
    a.dataset_category,
    b.start_date as last_sync_date,
    fn_get_mastername(b.job_status, 'QCD_JOB_STATUS') as last_sync_status,
	  a.system_flag,
    a.active_status,
    fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
  from recon_mst_tdataset a
  left join admin_mst_tdatasetcontext c on c.dataset_code=a.dataset_code
    and c.delete_flag = 'N'
  left join recon_trn_tjob as b on a.last_job_gid = b.job_gid and b.delete_flag = 'N'
  where true
  and (c.parent_master_syscode='QCD_L1' or a.system_flag = 'Y')
  and a.delete_flag = 'N'
  order by a.dataset_gid asc;
end $$

DELIMITER ;