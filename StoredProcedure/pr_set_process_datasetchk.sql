DELIMITER $$
DROP PROCEDURE IF EXISTS `pr_set_process_datasetchk` $$
CREATE procedure `pr_set_process_datasetchk`
(
  in in_scheduler_gid int,
  in in_ip_addr varchar (255),
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32),
  out out_msg text,
  out out_result int
)
me:begin
  /*
    Created By : Vijayavel
    Created Date : 19-11-2023

    Updated By : Vijayavel
    Updated Date : 26-03-2024

    Version : 3
  */
  declare v_pipeline_code text default '';
  declare v_dataset_code text default '';
  declare v_dataset_name text default '';
  declare v_dataset_db_name text default '';
  declare v_recon_code text default '';
  declare v_scheduled_date datetime default null;
  declare v_scheduler_param text default '';
  declare v_file_name text default '';
  declare v_job_input_param text default '';
  declare v_job_gid int default 0;
  declare v_dataset_table_name text default '';
  declare v_sql text default '';
  declare v_result int default 0;

  /*
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE,
    @errno = MYSQL_ERRNO, @text = MESSAGE_TEXT;

    SET @full_error = CONCAT("ERROR ", @errno, " (", @sqlstate, "): ", @text);

    call pr_upd_job(v_job_gid,'F',@full_error,@msg1,@result1);

    -- update in scheduler table
    update recon_trn_tscheduler set
      scheduler_status = 'F',
      update_date = sysdate(),
      update_by = in_user_code
    where scheduler_gid = in_scheduler_gid
    and delete_flag = 'N';

    -- set last job_gid
    update recon_mst_tdataset set
      last_job_gid = v_job_gid
    where dataset_code = v_dataset_code
    and delete_flag = 'N';

    set out_msg = @full_error;
    set out_result = 0;

    SIGNAL SQLSTATE '99999' SET MESSAGE_TEXT = @text, MYSQL_ERRNO = @errno;
  END;
  */

  set out_msg = '';

  -- scheduler_gid validation in connector
  if not exists(select scheduler_gid from con_trn_tscheduler
     where scheduler_gid = in_scheduler_gid and delete_flag = 'N') then
    set out_msg = concat(out_msg,'Invalid connector scheduler !,');
    set out_result = 0;

    leave me;
  end if;

  -- scheduler_gid validation in recon_trn_tscheduler
  /*
  if not exists(select scheduler_gid from recon_trn_tscheduler
     where scheduler_gid = in_scheduler_gid
     and scheduler_status = 'F'
     and delete_flag = 'N') then
    set out_msg = concat(out_msg,'Invalid recon scheduler !,');
    set out_result = 0;

    leave me;
  end if;
  */

  -- get dataset table name
  set v_dataset_db_name = fn_get_configvalue('dataset_db_name');

  -- get file details
  select
    a.pipeline_code,
    a.scheduled_date,
    a.file_name,
    a.dataset_code,
    c.dataset_name,
    a.scheduler_parameters,
    c.dataset_table_name
  into
    v_pipeline_code,
    v_scheduled_date,
    v_file_name,
    v_dataset_code,
    v_dataset_name,
    v_scheduler_param,
    v_dataset_table_name
  from con_trn_tscheduler as a
  /*
  inner join con_mst_tpipeline as b on a.pipeline_code = b.pipeline_code
    and b.delete_flag = 'N'
  */
  inner join recon_mst_tdataset as c on a.dataset_code = c.dataset_code
    and c.active_status = 'Y'
    and c.delete_flag = 'N'
  where a.scheduler_gid = in_scheduler_gid
  and a.delete_flag = 'N';

  -- set null values as blank
  set v_pipeline_code = ifnull(v_pipeline_code,'');
  set v_dataset_code = ifnull(v_dataset_code,'');
  set v_dataset_name = ifnull(v_dataset_name,'');
  set v_file_name = ifnull(v_file_name,'');
  set v_scheduler_param = ifnull(v_scheduler_param,'');
  set v_dataset_table_name = ifnull(v_dataset_table_name,'');

  select job_gid into v_job_gid from recon_trn_tjob
  where jobtype_code = 'S'
  and job_ref_gid = in_scheduler_gid
  and delete_flag = 'N';

  set v_job_gid = ifnull(v_job_gid,0);

  -- call pr_ins_job('','S',in_scheduler_gid,concat('Processing Scheduler File ',v_file_name),v_scheduler_param,in_user_code,in_ip_addr,'I','Initiated...',v_job_gid,@msg,@result);

  if v_job_gid = 0 then
    set out_msg = concat(out_msg,@msg);
    set out_result = 0;

    leave me;
  end if;

  -- update in scheduler table
  update recon_trn_tscheduler set
    job_gid = v_job_gid,
    scheduler_status = 'I',
    update_date = sysdate(),
    update_by = in_user_code
  where scheduler_gid = in_scheduler_gid
  and delete_flag = 'N';

  if v_dataset_code = 'ACCBALANCE' then
    set v_sql = concat("replace into recon_trn_taccbal (scheduler_gid,dataset_code,tran_date,bal_value,insert_date,insert_by)
      select
        scheduler_gid,dataset_code,tran_date,bal_debit*-1+bal_credit,sysdate(),'",in_user_code,"'
      from ",v_dataset_table_name, "
      where scheduler_gid = ",cast(in_scheduler_gid as nchar),"
      and delete_flag = 'N'");
  elseif v_dataset_code = 'KOMANUAL' then
    set v_sql = concat("insert into recon_trn_tmanualtran
      (
        scheduler_gid,match_gid,tran_gid,tranbrkp_gid,recon_code,dataset_code,ko_value,ko_acc_mode,ko_mult,ko_reason
      )
      select
        scheduler_gid,match_gid,tran_gid,tranbrkp_gid,recon_code,dataset_code,ko_value,ko_acc_mode,
        case
          when ko_acc_mode = 'D' then -1
          when ko_acc_mode = 'C' then 1
          when ko_acc_mode = 'V' then 1
          else 0
        end as ko_mult,
        ko_reason
      from ",v_dataset_table_name, "
      where scheduler_gid = ",cast(in_scheduler_gid as nchar),"
      and delete_flag = 'N'");
  elseif v_dataset_code = 'POSTMANUAL' then
    set v_sql = concat("insert into recon_trn_tmanualtranbrkp
      (
        scheduler_gid,tran_gid,tranbrkp_gid,recon_code,dataset_code,tranbrkp_gid,tran_gid,tranbrkp_value,tranbrkp_acc_mode
      )
      select
        scheduler_gid,recon_code,dataset_code,tranbrkp_gid,tran_gid,tranbrkp_value,tranbrkp_acc_mode
      from ",v_dataset_table_name,"
      where scheduler_gid = ",cast(in_scheduler_gid as nchar),"
      and delete_flag = 'N'");
  elseif v_dataset_code = 'THEMEMANUAL' then
    set v_sql = concat("insert into recon_trn_tthemeupdate
      (
        scheduler_gid,recon_code,dataset_code,tranbrkp_gid,tran_gid,theme_desc
      )
      select
        scheduler_gid,recon_code,dataset_code,tranbrkp_gid,tran_gid,theme_desc
      from ",v_dataset_table_name,"
      where scheduler_gid = ",cast(in_scheduler_gid as nchar),"
      and delete_flag = 'N'");
  else
    recon_block:begin
      declare recon_done int default 0;
      declare recon_cursor cursor for
        select a.recon_code from recon_mst_trecondataset as a
        inner join recon_mst_trecon as b on a.recon_code = b.recon_code
          and b.period_from <= curdate()
          and (b.period_to >= curdate()
          or b.until_active_flag = 'Y')
          and b.active_status = 'Y'
          and b.delete_flag = 'N'
        where a.dataset_code = v_dataset_code
        and a.active_status = 'Y'
        and a.delete_flag = 'N';
      declare continue handler for not found set recon_done=1;

      open recon_cursor;

      recon_loop: loop
        fetch recon_cursor into v_recon_code;

        if recon_done = 1 then leave recon_loop; end if;

        call pr_set_dataset_transfer(in_scheduler_gid,v_recon_code,v_job_gid,@msg,@result);
      end loop recon_loop;

      close recon_cursor;
    end recon_block;
  end if;

  if v_sql <> '' then
    call pr_run_sql2(v_sql,@msg2,@result2);

    if v_dataset_code = 'THEMEMANUAL' then
      call pr_set_themeupdate(in_scheduler_gid,in_user_code,in_role_code,in_lang_code,@msg,@result);
    end if;
  end if;

  select 'velu';
  leave me;


  -- set last job_gid
  update recon_mst_tdataset set
    last_job_gid = v_job_gid
  where dataset_code = v_dataset_code
  and delete_flag = 'N';

  -- get processed record count
  set v_sql = concat("select count(*) into @v_result from ",v_dataset_table_name,
         " where scheduler_gid = ",cast(in_scheduler_gid as nchar));

  call pr_run_sql2(v_sql,@msg2,@result2);

  set v_result = ifnull(@v_result,0);


  -- update in scheduler table
  update recon_trn_tscheduler set
    scheduler_status = 'C',
    update_date = sysdate(),
    update_by = in_user_code
  where scheduler_gid = in_scheduler_gid
  and delete_flag = 'N';

  -- update job status
  call pr_upd_job(v_job_gid,'C',concat('Imported ',cast(v_result as nchar),' record(s)'),@msg,@result);

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;