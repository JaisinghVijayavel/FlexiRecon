﻿DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_tranwithbrkpreport` $$
CREATE PROCEDURE `pr_run_tranwithbrkpreport`(
  in in_recon_code varchar(32),
  in in_job_gid int,
  in in_rptsession_gid int,
  in in_condition text,
  in in_sorting_order text,
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
  /*
    Created By : Vijayavel
    Created Date : 08-02-2024

    Updated By : Vijayavel
    updated Date : 28-02-2024

    Version : 2
  */

  declare v_tran_field text default '';
  declare v_tranbrkp_field text default '';
  declare v_recontype_code text default '';

  declare v_count int default 0;
  declare v_sql text default '';

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  -- get table column
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

  if in_job_gid > 0 then
    select recontype_code into v_recontype_code from recon_mst_trecon
    where recon_code = (select recon_code from recon_trn_tjob where job_gid = in_job_gid)
    and active_status = 'Y'
    and delete_flag = 'N';
  else
    select recontype_code into v_recontype_code from recon_mst_trecon
    where recon_code = in_recon_code
    and active_status = 'Y'
    and delete_flag = 'N';
  end if;

  set v_recontype_code = ifnull(v_recontype_code,'');

  -- transfer tran records to report table
  set v_sql = concat('insert into recon_rpt_ttranwithbrkp(rptsession_gid,job_gid,user_code,dataset_name,',v_tran_field,') ');
  set v_sql = concat(v_sql,'select ');
  set v_sql = concat(v_sql,cast(in_rptsession_gid as nchar),' as rptsession_gid,');
  set v_sql = concat(v_sql,cast(in_job_gid as nchar),' as job_gid,');
  set v_sql = concat(v_sql,char(39),in_user_code,char(39),' as user_code,');
  set v_sql = concat(v_sql,'b.dataset_name,');
  set v_sql = concat(v_sql,concat('a.',replace(v_tran_field,',',',a.')),' from recon_trn_ttran as a ');
  set v_sql = concat(v_sql,'left join recon_mst_tdataset as b on a.dataset_code = b.dataset_code ');
  /*
  set v_sql = concat(v_sql,'left join recon_trn_ttranbrkp as s on 1 = 2 ');
  set v_sql = concat(v_sql,'left join recon_mst_tdataset as c on 1 = 2 ');
  */
  set v_sql = concat(v_sql,'where true ');

  set v_sql = concat(v_sql,in_condition,' ');

  if v_recontype_code = 'N' then
    set v_sql = concat(v_sql,'and a.ko_gid = 0 ');
  end if;

  set v_sql = concat(v_sql,'and a.delete_flag = ''N'' ');
  set v_sql = concat(v_sql,' ',in_sorting_order);

  call pr_run_sql(v_sql,@out_msg,@out_result);

  -- update tranbrkp_dataset_code, tranbrkp_dataset_name
  set v_sql = concat('update recon_rpt_ttranwithbrkp set ');
  set v_sql = concat(v_sql,'tranbrkp_dataset_code = dataset_code,');
  set v_sql = concat(v_sql,'tranbrkp_dataset_name = dataset_name ');
  set v_sql = concat(v_sql,'where true ');
  set v_sql = concat(v_sql,'and job_gid = ',cast(in_job_gid as nchar),' ');
  set v_sql = concat(v_sql,'and rptsession_gid = ',cast(in_rptsession_gid as nchar),' ');

  call pr_run_sql(v_sql,@out_msg,@out_result);

  -- transfer tranbrkp records to report table
  set v_sql = concat('insert into recon_rpt_ttranwithbrkp(rptsession_gid,job_gid,user_code,dataset_name,tranbrkp_dataset_name,');
  set v_sql = concat(v_sql,'base_tran_value,base_excp_value,base_acc_mode,');
  set v_sql = concat(v_sql,v_tranbrkp_field,') ');
  set v_sql = concat(v_sql,'select ');
  set v_sql = concat(v_sql,cast(in_rptsession_gid as nchar),' as rptsession_gid,');
  set v_sql = concat(v_sql,cast(in_job_gid as nchar),' as job_gid,');
  set v_sql = concat(v_sql,char(39),in_user_code,char(39),' as user_code,');
  set v_sql = concat(v_sql,'b.dataset_name,');
  set v_sql = concat(v_sql,'c.dataset_name,');
  set v_sql = concat(v_sql,'a.tran_value,a.excp_value,a.tran_acc_mode,');
  set v_sql = concat(v_sql,concat('s.',replace(v_tranbrkp_field,',',',s.')),' from recon_trn_ttranbrkp as s ');
  set v_sql = concat(v_sql,'left join recon_mst_tdataset as b on s.dataset_code = b.dataset_code ');
  set v_sql = concat(v_sql,'left join recon_mst_tdataset as c on s.tranbrkp_dataset_code = c.dataset_code ');
  set v_sql = concat(v_sql,'left join recon_trn_ttran as a on s.tran_gid = a.tran_gid ');
  set v_sql = concat(v_sql,'where true ');
  set v_sql = concat(v_sql,replace(in_condition,'a.','s.'),' ');

  if v_recontype_code <> 'N' then
    set v_sql = concat(v_sql,'and s.excp_value <> 0 ');
  end if;

  set v_sql = concat(v_sql,'and s.tran_gid > 0 ');
  set v_sql = concat(v_sql,'and s.delete_flag = ''N'' ');
  set v_sql = concat(v_sql,' ',replace(in_sorting_order,'a.','s.'));

  call pr_run_sql(v_sql,@out_msg,@out_result);

  -- transfer tranbrkp records to report table - not posted cases
  set v_sql = concat('insert into recon_rpt_ttranwithbrkp(rptsession_gid,job_gid,user_code,dataset_name,tranbrkp_dataset_name,');
  set v_sql = concat(v_sql,'base_tran_value,base_excp_value,base_acc_mode,');
  set v_sql = concat(v_sql,v_tranbrkp_field,') ');
  set v_sql = concat(v_sql,'select ');
  set v_sql = concat(v_sql,cast(in_rptsession_gid as nchar),' as rptsession_gid,');
  set v_sql = concat(v_sql,cast(in_job_gid as nchar),' as job_gid,');
  set v_sql = concat(v_sql,char(39),in_user_code,char(39),' as user_code,');
  set v_sql = concat(v_sql,'b.dataset_name,');
  set v_sql = concat(v_sql,'c.dataset_name,');
  set v_sql = concat(v_sql,'a.tran_value,a.excp_value,a.tran_acc_mode,');
  set v_sql = concat(v_sql,concat('s.',replace(v_tranbrkp_field,',',',s.')),' from recon_trn_ttranbrkp as s ');
  set v_sql = concat(v_sql,'left join recon_mst_tdataset as b on s.dataset_code = b.dataset_code ');
  set v_sql = concat(v_sql,'left join recon_mst_tdataset as c on s.tranbrkp_dataset_code = c.dataset_code ');
  set v_sql = concat(v_sql,'left join recon_trn_ttran as a on s.tran_gid = a.tran_gid ');
  set v_sql = concat(v_sql,'where true ');
  set v_sql = concat(v_sql,replace(in_condition,'a.','s.'),' ');

  if v_recontype_code <> 'N' then
    set v_sql = concat(v_sql,'and s.excp_value <> 0 ');
  end if;

  set v_sql = concat(v_sql,'and s.tran_gid = 0 ');
  set v_sql = concat(v_sql,'and s.delete_flag = ''N'' ');
  set v_sql = concat(v_sql,' ',replace(in_sorting_order,'a.','s.'));

  call pr_run_sql(v_sql,@out_msg,@out_result);

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;