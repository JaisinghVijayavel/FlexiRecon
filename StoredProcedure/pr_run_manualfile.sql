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


  select
    b.target_dataset_code into v_dataset_code
  from con_trn_tscheduler as a
  inner join con_mst_tpipeline as b on a.pipeline_code = b.pipeline_code and b.delete_flag = 'N'
  where a.scheduler_gid = in_scheduler_gid
  and a.delete_flag = 'N';

  set v_dataset_code = ifnull(v_dataset_code,'');

  if v_dataset_code = 'KOMANUAL' then
    call pr_run_manualmatchfile(in_scheduler_gid,in_ip_addr,in_user_code,@out_msg,@out_result);
  elseif v_dataset_code = 'POSTMANUAL' then
    call pr_run_manualpostfile(in_scheduler_gid,in_ip_addr,in_user_code,@out_msg,@out_result);
  else
    set @out_msg = 'Invalid scheduler !';
    set @out_result = 0;
  end if;

  set out_msg = @out_msg;
  set out_result = @out_result;
end $$

DELIMITER ;