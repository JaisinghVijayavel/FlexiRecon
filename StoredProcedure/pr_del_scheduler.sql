DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_del_scheduler` $$
CREATE PROCEDURE `pr_del_scheduler`(
  in in_scheduler_gid int,
  in in_remark varchar(255),
  in in_user_code varchar(32),
  out out_result int,
  out out_msg text
)
me:begin
  /*
    Created By - Vijayavel
    Created Date - 14-03-2025

    Updated By - Vijayavel
    Updated Date -

	  Version - 001
	*/

  declare v_dataset_code text default '';
  declare v_dataset_db_name text default '';
  declare v_sql text default '';

  declare err_msg text default '';
  declare err_flag boolean default false;

  if exists(select scheduler_gid from recon_trn_tscheduler
    where scheduler_gid = in_scheduler_gid
    and scheduler_status in ('C','F')
    and delete_flag = 'N') then
    select
      b.target_dataset_code into v_dataset_code
    from con_trn_tscheduler as a
    inner join con_mst_tpipeline as b on a.pipeline_code = b.pipeline_code
    where a.scheduler_gid = in_scheduler_gid
    and a.delete_flag = 'N';
  else
    set err_flag = true;
    set err_msg = concat(err_msg,'Invalid scheduler !,');
  end if;

  if v_dataset_code = 'KOMANUAL' then
    if exists
      (
        select a.tran_gid from recon_trn_tmanualtran as a
        where a.scheduler_gid = in_scheduler_gid
        and a.ko_status <> 'P'
        and a.delete_flag = 'N'
      ) or exists
      (
        select a.tran_gid from recon_trn_tmanualtranmatch as a
        where a.scheduler_gid = in_scheduler_gid
        and a.ko_status <> 'P'
        and a.delete_flag = 'N'
      ) then
      set err_flag = true;
      set err_msg = concat(err_msg,'Access denied !');
    end if;
  elseif v_dataset_code = 'POSTMANUAL' then
    if exists
      (
        select a.tran_gid from recon_trn_tmanualtranbrkp as a
        where a.scheduler_gid = in_scheduler_gid
        and a.tranbrkp_status <> 'P'
        and a.delete_flag = 'N'
      ) or exists
      (
        select a.tran_gid from recon_trn_tmanualtranbrkppost as a
        where a.scheduler_gid = in_scheduler_gid
        and a.tranbrkp_status <> 'P'
        and a.delete_flag = 'N'
      ) then
      set err_flag = true;
      set err_msg = concat(err_msg,'Access denied !');
    end if;
  else
    if exists
      (
        select a.tran_gid from recon_trn_ttran as a
        where a.scheduler_gid = in_scheduler_gid
        and (a.excp_value <> a.tran_value or a.mapped_value > 0)
        and a.delete_flag = 'N'
      ) or exists
      (
        select a.tran_gid from recon_trn_ttranko as a
        where a.scheduler_gid = in_scheduler_gid
        and a.delete_flag = 'N'
      ) or exists
      (
        select a.tranbrkp_gid from recon_trn_ttranbrkp as a
        where a.scheduler_gid = in_scheduler_gid
        and a.tran_gid > 0
        and a.delete_flag = 'N' limit 1
      ) or exists
      (
        select a.tranbrkp_gid from recon_trn_ttranbrkpko as a
        where a.scheduler_gid = in_scheduler_gid
        and a.delete_flag = 'N' limit 1
      ) then
      set err_flag = true;
      set err_msg = concat(err_msg,'Access denied !');
    end if;
  end if;

  if err_flag = true then
    set out_result = 0;
    set out_msg = err_msg;
    leave me;
  end if;

  -- update in scheduler table
  update recon_trn_tscheduler set
    scheduler_status = 'D',
    scheduler_remark = in_remark,
    update_date = sysdate(),
    update_by = in_user_code
  where scheduler_gid = in_scheduler_gid
  and delete_flag = 'N';


  if v_dataset_code = 'KOMANUAL' then
    delete from recon_trn_tmanualtran where scheduler_gid = in_scheduler_gid;
  elseif v_dataset_code = 'POSTMANUAL' then
    delete from recon_trn_tmanualtranbrkp where scheduler_gid = in_scheduler_gid;
  elseif v_dataset_code = 'ACCBALANCE' then
    delete from recon_trn_taccbal where scheduler_gid = in_scheduler_gid;
  else
    delete from recon_trn_ttran where scheduler_gid = in_scheduler_gid;
    delete from recon_trn_ttranbrkp where scheduler_gid = in_scheduler_gid;
  end if;

  -- get dataset database name
  set v_dataset_db_name = fn_get_configvalue('dataset_db_name');
  set v_dataset_db_name = ifnull(v_dataset_db_name,'');

  if v_dataset_db_name <> '' then
    set v_dataset_code = concat(v_dataset_db_name,'.',v_dataset_code);
  end if;

  set v_sql = concat("delete from ",
    v_dataset_code,"
    where scheduler_gid = ",cast(in_scheduler_gid as nchar),"
    and delete_flag = 'N'
  ");

  call pr_run_sql1(v_sql,@msg,@result);

  set out_result = 1;
  set out_msg = 'File deleted successfully !';
end $$

DELIMITER ;