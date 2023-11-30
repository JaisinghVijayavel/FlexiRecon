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