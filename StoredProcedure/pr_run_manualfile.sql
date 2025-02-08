DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_manualfile` $$
CREATE PROCEDURE `pr_run_manualfile`
(
  in in_scheduler_gid int,
  in in_ip_addr varchar(255),
  in in_user_code varchar(16),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_dataset_code text default '';
  declare v_file_name text default '';
  declare v_job_gid int default 0;
  declare v_txt text default '';

  select
    b.target_dataset_code,a.file_name into v_dataset_code,v_file_name
  from con_trn_tscheduler as a
  inner join con_mst_tpipeline as b on a.pipeline_code = b.pipeline_code and b.delete_flag = 'N'
  where a.scheduler_gid = in_scheduler_gid
  and a.delete_flag = 'N';

  set v_dataset_code = ifnull(v_dataset_code,'');

  if v_dataset_code = 'KOMANUAL' then
    call pr_run_manualmatchfile(in_scheduler_gid,in_ip_addr,in_user_code,@out_msg,@out_result);
  elseif v_dataset_code = 'POSTMANUAL' then
    call pr_run_manualpostfile(in_scheduler_gid,'',in_ip_addr,in_user_code,@out_msg,@out_result);
  elseif v_dataset_code = 'THEMEMANUAL'
    or v_dataset_code = 'FIELDUPDATE'
    or v_dataset_code = 'IUTENTRY' then
		if exists(select job_gid from recon_trn_tjob
			where jobtype_code = 'M'
			and job_status in ('I','P')
			and delete_flag = 'N') then

			select group_concat(cast(job_gid as nchar)) into v_txt from recon_trn_tjob
			where jobtype_code = 'M'
			and job_status in ('I','P')
			and delete_flag = 'N';

			set out_msg = concat('Manual match is already running in the job id ', v_txt ,' ! ');
			set out_result = 0;

			set v_job_gid = 0;

			leave me;
		end if;

    if v_dataset_code = 'THEMEMANUAL' then
	    call pr_ins_job('','M',in_scheduler_gid,concat('Theme manual - ',v_file_name),v_file_name,
        in_user_code,in_ip_addr,'I','Initiated...',@out_job_gid,@msg,@result);

      set v_job_gid = @out_job_gid;

      call pr_set_themeupdate(in_scheduler_gid,in_user_code,'','',@out_msg,@out_result);
    elseif v_dataset_code = 'FIELDUPDATE' then
	    call pr_ins_job('','M',in_scheduler_gid,concat('Field update - ',v_file_name),v_file_name,
        in_user_code,in_ip_addr,'I','Initiated...',@out_job_gid,@msg,@result);

      set v_job_gid = @out_job_gid;

      call pr_set_fieldupdate(in_scheduler_gid,in_user_code,'','',@out_msg,@out_result);
    elseif v_dataset_code = 'IUTENTRY' then
	    call pr_ins_job('','M',in_scheduler_gid,concat('IUT entry - ',v_file_name),v_file_name,
        in_user_code,in_ip_addr,'I','Initiated...',@out_job_gid,@msg,@result);

      set v_job_gid = @out_job_gid;

      call pr_run_iutentry(in_scheduler_gid,v_job_gid,@out_msg,@out_result);
    end if;

    call pr_upd_job(v_job_gid,'C','Completed',@msg,@result);
  else
    set @out_msg = 'Invalid scheduler !';
    set @out_result = 0;
  end if;

  set out_msg = @out_msg;
  set out_result = @out_result;
end $$

DELIMITER ;