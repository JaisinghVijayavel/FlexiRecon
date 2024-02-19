DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_tranwithbrkpreport` $$
CREATE PROCEDURE `pr_run_tranwithbrkpreport`(
  in in_job_gid int,
  in in_rptsession_gid int,
  in in_condition text,
  in in_user_code varchar(16),
  out out_msg text,
  out out_result int
)
me:BEGIN
  /*
    Created By : Vijayavel
    Created Date : 08-02-2024

    Updated By : Vijayavel
    updated Date :

    Version : 1
  */

  declare v_tran_field text default '';
  declare v_tranbrkp_field text default '';
  declare v_recontype_code text default '';

  declare v_sql text default '';

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  SELECT
	  group_concat(t.COLUMN_NAME) into v_tran_field
  FROM information_schema.columns as t
  WHERE t.table_schema=database()
  AND t.table_name = 'recon_trn_ttran';

  SELECT
	  group_concat(t.COLUMN_NAME) into v_tranbrkp_field
  FROM information_schema.columns as t
  WHERE t.table_schema=database()
  AND t.table_name = 'recon_trn_ttranbrkp';

  select recontype_code into v_recontype_code from recon_mst_trecon
  where recon_code = (select recon_code from recon_trn_tjob where job_gid = in_job_gid)
  and active_status = 'Y'
  and delete_flag = 'N';

  set v_recontype_code = ifnull(v_recontype_code,'');

  set v_sql = concat('insert into recon_rpt_ttranwithbrkp(rptsession_gid,job_gid,dataset_name,',v_tran_field,') ');
  set v_sql = concat(v_sql,'select ');
  set v_sql = concat(v_sql,cast(in_rptsession_gid as nchar),' as rptsession_gid,');
  set v_sql = concat(v_sql,cast(in_job_gid as nchar),' as job_gid,');
  set v_sql = concat(v_sql,'b.dataset_name,');
  set v_sql = concat(v_sql,concat('a.',replace(v_tran_field,',',',a.')),' from recon_trn_ttran as a ');
  set v_sql = concat(v_sql,'left join recon_mst_tdataset as b on a.dataset_code = b.dataset_code ');
  set v_sql = concat(v_sql,'where true ');
  set v_sql = concat(v_sql,in_condition,' ');

  if v_recontype_code <> 'N' then
    set v_sql = concat(v_sql,'and a.excp_value > 0 ');
  else
    set v_sql = concat(v_sql,'and a.ko_gid = 0 ');
  end if;

  set v_sql = concat(v_sql,'and a.delete_flag = ''N'' ');

  call pr_run_sql(v_sql,@out_msg,@out_result);

  set v_sql = concat('insert into recon_rpt_ttranwithbrkp(rptsession_gid,job_gid,dataset_name,tranbrkp_dataset_name,');
  set v_sql = concat(v_sql,'base_tran_value,base_excp_value,base_acc_mode,');
  set v_sql = concat(v_sql,v_tranbrkp_field,') ');
  set v_sql = concat(v_sql,'select ');
  set v_sql = concat(v_sql,cast(in_rptsession_gid as nchar),' as rptsession_gid,');
  set v_sql = concat(v_sql,cast(in_job_gid as nchar),' as job_gid,');
  set v_sql = concat(v_sql,'b.dataset_name,');
  set v_sql = concat(v_sql,'c.dataset_name,');
  set v_sql = concat(v_sql,'d.tran_value,d.excp_value,d.tran_acc_mode,');
  set v_sql = concat(v_sql,concat('a.',replace(v_tranbrkp_field,',',',a.')),' from recon_trn_ttranbrkp as a ');
  set v_sql = concat(v_sql,'left join recon_mst_tdataset as b on a.dataset_code = b.dataset_code ');
  set v_sql = concat(v_sql,'left join recon_mst_tdataset as c on a.tranbrkp_dataset_code = c.dataset_code ');
  set v_sql = concat(v_sql,'left join recon_trn_ttran as d on a.tran_gid = d.tran_gid ');
  set v_sql = concat(v_sql,'where true ');
  set v_sql = concat(v_sql,in_condition,' ');

  if v_recontype_code <> 'N' then
    set v_sql = concat(v_sql,'and a.excp_value > 0 ');
    set v_sql = concat(v_sql,'and a.tran_gid > 0 ');
  else
    set v_sql = concat(v_sql,'and 1 = 2 ');
  end if;

  set v_sql = concat(v_sql,'and a.delete_flag = ''N'' ');

  call pr_run_sql(v_sql,@out_msg,@out_result);

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;