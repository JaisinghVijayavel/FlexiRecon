DELIMITER $$

drop trigger if exists trg_con_trn_tscheduler_after_update $$
create trigger trg_con_trn_tscheduler_after_update after update on con_trn_tscheduler
for each row
begin
	if New.scheduler_status = 'Completed' then
		insert ignore into recon_trn_tscheduler
		(
			scheduler_gid,scheduler_status,insert_by,insert_date
		)
		select New.scheduler_gid,'S',New.scheduler_initiated_by,sysdate();

    -- call pr_set_process_dataset(New.scheduler_gid,'',New.scheduler_initiated_by,'','',@msg,@result);
  elseif (New.scheduler_status = 'Scheduled' or New.scheduler_status = 'Locked') and New.scheduler_start_date <= sysdate() then
    call pr_ins_job('','S',New.scheduler_gid,concat('Processing Scheduler ',ifnull(New.file_name,'')),'',
      New.scheduler_initiated_by,'','I','Initiated',@job_gid,@msg,@result);
  elseif New.scheduler_status = 'Failed' then
    select job_gid into @job_gid from recon_trn_tjob
    where jobtype_code = 'S'
    and job_ref_gid = New.scheduler_gid
    and delete_flag = 'N' limit 0,1;

    set @job_gid = ifnull(@job_gid,0);

    if @job_gid > 0 then
      call pr_upd_job(@job_gid,'F','Failed in connector',@msg1,@result1);
    else
      -- insert into scheduler as failed one
		  insert ignore into recon_trn_tscheduler
		  (
			  scheduler_gid,scheduler_status,insert_by,insert_date
		  )
		  select New.scheduler_gid,'F',New.scheduler_initiated_by,sysdate();

      -- insert as failed job
      call pr_ins_job('','S',New.scheduler_gid,concat('Processing Scheduler ',ifnull(New.file_name,'')),'',
        New.scheduler_initiated_by,'','F','Failed',@job_gid,@msg,@result);
    end if;
  else
    if exists (select scheduler_gid from recon_trn_tscheduler
      where scheduler_gid = New.scheduler_gid
      and scheduler_status = 'S'
      and delete_flag = 'N') then
      delete from recon_trn_tscheduler
      where scheduler_gid = New.scheduler_gid
      and scheduler_status = 'S'
      and delete_flag = 'N';
    end if;
	end if;
end $$

DELIMITER ;