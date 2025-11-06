DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_manualfile` $$
CREATE PROCEDURE `pr_run_manualfile`
(
  in in_scheduler_gid int,
  in in_ip_addr varchar(255),
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
  /*
    Created By : Vijayavel
    Created Date :

    Updated By : Vijayavel
    updated Date : 21-08-2025

    Version : 1
  */

  declare v_dataset_code text default '';
  declare v_file_name text default '';
  declare v_job_gid int default 0;
  declare v_txt text default '';
  declare v_result int default 0;
  declare v_job_status text default '';
  declare v_msg text default '';

  select
    a.dataset_code,a.file_name into v_dataset_code,v_file_name
  from con_trn_tscheduler as a
  where a.scheduler_gid = in_scheduler_gid
  and a.delete_flag = 'N';

  set v_dataset_code = ifnull(v_dataset_code,'');

  set @out_msg = 'Failed';
  set @out_result = 0;

  if v_dataset_code = 'KOMANUAL' then
	  call pr_ins_job('','M',in_scheduler_gid,concat('Manual match - ',v_file_name),v_file_name,
      in_user_code,in_ip_addr,'I','Initiated...',@out_job_gid,@msg,@result);

    set v_job_gid = @out_job_gid;

    call pr_run_manualmatchfile(in_scheduler_gid,v_job_gid,in_ip_addr,in_user_code,@out_msg,@out_result);
  elseif v_dataset_code = 'POSTMANUAL' then
	  call pr_ins_job('','M',in_scheduler_gid,concat('Manual posting - ',v_file_name),v_file_name,
      in_user_code,in_ip_addr,'I','Initiated...',@out_job_gid,@msg,@result);

    set v_job_gid = @out_job_gid;

    call pr_run_manualpostfile(in_scheduler_gid,v_job_gid,in_ip_addr,in_user_code,@out_msg,@out_result);
  elseif v_dataset_code = 'THEMEMANUAL'
    or v_dataset_code = 'FIELDUPDATE'
    or v_dataset_code = 'IUTFIELDUPDATE'
    or v_dataset_code = 'IUTENTRY' then
    if v_dataset_code = 'THEMEMANUAL' then
	    call pr_ins_job('','M',in_scheduler_gid,concat('Theme manual - ',v_file_name),v_file_name,
        in_user_code,in_ip_addr,'I','Initiated...',@out_job_gid,@msg,@result);

      set v_job_gid = @out_job_gid;

      call pr_set_themeupdate(in_scheduler_gid,v_job_gid,in_user_code,'','',@out_msg,@out_result);
    elseif v_dataset_code = 'FIELDUPDATE' then
	    call pr_ins_job('','M',in_scheduler_gid,concat('Field update - ',v_file_name),v_file_name,
        in_user_code,in_ip_addr,'I','Initiated...',@out_job_gid,@msg,@result);

      set v_job_gid = @out_job_gid;

      call pr_set_fieldupdate(in_scheduler_gid,v_job_gid,in_user_code,'','',@out_msg,@out_result);
    elseif v_dataset_code = 'IUTENTRY' then
	    call pr_ins_job('','M',in_scheduler_gid,concat('IUT entry - ',v_file_name),v_file_name,
        in_user_code,in_ip_addr,'I','Initiated...',@out_job_gid,@msg,@result);

      set v_job_gid = @out_job_gid;

      call pr_run_iutentry(in_scheduler_gid,v_job_gid,@out_msg,@out_result);
    elseif v_dataset_code = 'IUTFIELDUPDATE' then
	    call pr_ins_job('','M',in_scheduler_gid,concat('IUT field update - ',v_file_name),v_file_name,
        in_user_code,in_ip_addr,'I','Initiated...',@out_job_gid,@msg,@result);

      set v_job_gid = @out_job_gid;

      call pr_set_iutfieldupdate(in_scheduler_gid,v_job_gid,in_user_code,'','',@out_msg,@out_result);
    end if;

    set v_result= @out_result;
    set v_msg = @out_msg;

    if v_result <> 0 then
      set v_job_status = 'C';
    else
      set v_job_status = 'F';
    end if;

    call pr_upd_job(v_job_gid,v_job_status,v_msg,@out_msg,@out_result);
  else
    set @out_msg = 'Invalid scheduler !';
    set @out_result = 0;
  end if;

  set out_msg = @out_msg;
  set out_result = @out_result;
end $$

DELIMITER ;