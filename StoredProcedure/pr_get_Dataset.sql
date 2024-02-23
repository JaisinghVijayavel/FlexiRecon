DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_Dataset` $$
CREATE PROCEDURE `pr_get_Dataset`(
  in in_user_gid int,
  in in_active_status  varchar(32),
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32)
)
begin
  declare v_app_datetime_format text default '';

  set v_app_datetime_format = fn_get_configvalue('app_datetime_format');

  if in_user_gid < 1 then
    set in_user_gid = null;
  end if;

  if in_active_status <> 'Y' and in_active_status <> 'N' then
    set in_active_status = null;
  end if;

  set @row_number := 0;

  select
    (@row_number := @row_number + 1) as sl_no,
    a.dataset_gid,
    a.dataset_code,
    a.dataset_name,
    a.dataset_category,
    date_format(b.start_date,v_app_datetime_format) as last_sync_date,
    date_format(b.end_date,v_app_datetime_format) as completed_date,
    b.job_remark,
    fn_get_mastername(b.job_status, 'QCD_JOB_STATUS') as last_sync_status,
    a.system_flag,
    a.active_status,
    fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
  from recon_mst_tdataset a
  inner join admin_mst_tdatasetcontext c on c.dataset_code = a.dataset_code
    and c.parent_master_syscode='QCD_L1'
    and c.delete_flag = 'N'
  left join recon_trn_tjob as b on a.last_job_gid = b.job_gid and b.delete_flag = 'N'
  where a.delete_flag = 'N'
  order by a.dataset_gid desc;
end $$

DELIMITER ;