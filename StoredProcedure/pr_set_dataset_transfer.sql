﻿DELIMITER $$
DROP PROCEDURE IF EXISTS `pr_set_dataset_transfer` $$
CREATE procedure `pr_set_dataset_transfer`
(
  in in_scheduler_gid int,
  in in_recon_code text,
  in in_job_gid int,
  out out_msg text,
  out out_result int
)
me:begin
  /*
    Created By : Vijayavel
    Created Date : 06-10-2023

    Updated By : Vijayavel
    Updated Date : 31-07-2025

    Version : 8
  */

  declare v_pipeline_code text default '';
  declare v_dataset_code text default '';
  declare v_parent_dataset_code text default '';
  declare v_dataset_table_name text default '';
  declare v_source_db_type text default '';

  declare v_recontype_code text default '';
  declare v_recon_value_flag text default '';
  declare v_recon_value_field text default '';
  declare v_recon_date_flag text default '';
  declare v_recon_date_field text default '';
  declare v_dataset_type text default '';

  declare v_recon_closure_date text default '';
  declare v_tran_date_field text default '';
  declare v_recon_condition text default '';

  declare v_valuetype_code text default '';
  declare v_bal_valuetype_code text default '';

  declare v_source_sql text default '';
  declare v_target_sql text default '';
  declare v_sql text default '';

  declare v_target_table text default '';

  declare v_tran_date_format text default '';
  declare v_iis_date_format text default '';
  declare v_job_gid int default 0;

  declare v_bcpfield_all text default '';
  declare v_tranfield_all text default '';

  declare v_user_code text default '';
  declare v_count int default 0;

  declare v_concurrent_ko_flag text default '';

  drop temporary table if exists recon_tmp_ttrangid;
  drop temporary table if exists recon_tmp_tbalance;


  CREATE temporary TABLE recon_tmp_ttrangid
  (
    tran_gid int unsigned NOT NULL,
    tran_date date not null,
    PRIMARY KEY (tran_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_tbalance
  (
    tran_date date not null,
    dataset_code varchar(32) not null,
    bal_value_debit double(15,2) not null default 0,
    bal_value_credit double(15,2) not null default 0,
    PRIMARY KEY (tran_date)
  ) ENGINE = MyISAM;


  set v_job_gid = in_job_gid;

  -- scheduler_gid validation
  if not exists(select scheduler_gid from con_trn_tscheduler
     where scheduler_gid = in_scheduler_gid and delete_flag = 'N') then
    set out_msg = concat(out_msg,'Invalid scheduler !,');
    set out_result = 0;
    leave me;
  end if;

  -- get file details
  select
    a.pipeline_code,
    a.dataset_code,
    c.source_db_type,
    d.dataset_table_name,
    a.scheduler_initiated_by
  into
    v_pipeline_code,
    v_dataset_code,
    v_source_db_type,
    v_dataset_table_name,
    v_user_code
  from con_trn_tscheduler as a
  inner join con_mst_tpipeline as b on a.pipeline_code = b.pipeline_code
    and b.delete_flag = 'N'
  inner join con_mst_tconnection as c on b.connection_code = c.connection_code
    and c.delete_flag = 'N'
  inner join recon_mst_tdataset as d on a.dataset_code = d.dataset_code
    and d.active_status = 'Y'
    and d.delete_flag = 'N'
  where a.scheduler_gid = in_scheduler_gid
  and a.delete_flag = 'N';

  -- set null values as blank
  set v_pipeline_code = ifnull(v_pipeline_code,'');
  set v_dataset_code = ifnull(v_dataset_code,'');
  set v_source_db_type = ifnull(v_source_db_type,'');
  set v_dataset_table_name = ifnull(v_dataset_table_name,'');
  set v_user_code = ifnull(v_user_code,'');

  -- recon validation
  if not exists(select recon_code from recon_mst_trecon
    where recon_code = in_recon_code
    and period_from <= curdate()
    and (until_active_flag = 'Y'
    or period_to >= curdate())
    and active_status = 'Y'
    and delete_flag = 'N') then

    set out_msg = concat(out_msg,'Invalid recon code !');
    set out_result = 0;
    leave me;
  end if;

  -- get recon value
  select
    a.recontype_code,
    a.recon_value_flag,
    a.recon_value_field,
    a.recon_date_flag,
    a.recon_date_field,
    cast(a.recon_closure_date as nchar),
    b.dataset_type,
    b.dataset_code,
    b.parent_dataset_code,
    b.valuetype_code,
    b.bal_valuetype_code
  into
    v_recontype_code,
    v_recon_value_flag,
    v_recon_value_field,
    v_recon_date_flag,
    v_recon_date_field,
    v_recon_closure_date,
    v_dataset_type,
    v_dataset_code,
    v_parent_dataset_code,
    v_valuetype_code,
    v_bal_valuetype_code
  from recon_mst_trecon as a
  inner join recon_mst_trecondataset as b on a.recon_code = b.recon_code
    and b.dataset_code = v_dataset_code
    and b.dataset_type in ('B','T','S')
    and b.active_status = 'Y'
    and b.delete_flag = 'N'
  where a.recon_code = in_recon_code
  and a.period_from <= curdate()
  and (a.until_active_flag = 'Y'
  or a.period_to >= curdate())
  and a.active_status = 'Y'
  and a.delete_flag = 'N';

  -- recon variable values set blank for null cases
  set v_recontype_code = ifnull(v_recontype_code,'');
  set v_recon_value_flag = ifnull(v_recon_value_flag,'');
  set v_recon_value_field = ifnull(v_recon_value_field,'');
  set v_recon_date_flag = ifnull(v_recon_date_flag,'');
  set v_recon_date_field = ifnull(v_recon_date_field,'');
  set v_recon_closure_date = ifnull(v_recon_closure_date,'');

  set v_dataset_type = ifnull(v_dataset_type,'');
  set v_dataset_code = ifnull(v_dataset_code,'');
  set v_parent_dataset_code = ifnull(v_parent_dataset_code,'');
  set v_valuetype_code = ifnull(v_valuetype_code,'');
  set v_bal_valuetype_code = ifnull(v_bal_valuetype_code,'');

  -- get iis server date format
  set v_iis_date_format = ifnull(fn_get_configvalue('iis_date_format'),'');

  /*
  if (v_recontype_code = 'W' or v_recontype_code = 'I' or v_recontype_code = 'B')
    and v_recon_closure_date <> '' then

    select
      dataset_field_name into v_tran_date_field
    from recon_mst_treconfieldmapping
    where recon_code = in_recon_code
    and dataset_code = v_dataset_code
    and recon_field_name = 'tran_date'
    and active_status = 'Y'
    and delete_flag = 'N';

    if v_tran_date_field <> '' then
      set v_recon_condition = concat(" and cast(",v_tran_date_field ,"as date) > '",v_recon_closure_date,"' ");
    end if;
  end if;
  */

  -- concurrent KO flag
  set v_concurrent_ko_flag = fn_get_configvalue('concurrent_ko_flag');

  if v_dataset_type = 'B' or v_dataset_type = 'T' then
    if v_concurrent_ko_flag = 'Y' then
      set v_target_table = concat(in_recon_code,'_tran');
    else
      set v_target_table = 'recon_trn_ttran';
    end if;

    set v_source_sql = concat('select ',cast(in_scheduler_gid as nchar),',');
    set v_source_sql = concat(v_source_sql,char(34),in_recon_code,char(34),',');
    set v_source_sql = concat(v_source_sql,char(34),v_dataset_code,char(34),',');

    set v_target_sql = concat('insert into ',v_target_table,' (scheduler_gid,recon_code,dataset_code,');
  elseif v_dataset_type = 'S' then
    if v_concurrent_ko_flag = 'Y' then
      set v_target_table = concat(in_recon_code,'_tranbrkp');
    else
      set v_target_table = 'recon_trn_ttranbrkp';
    end if;

    set v_source_sql = concat('select ',cast(in_scheduler_gid as nchar),',');
    set v_source_sql = concat(v_source_sql,char(34),in_recon_code,char(34),',');
    set v_source_sql = concat(v_source_sql,char(34),v_parent_dataset_code,char(34),',');
    set v_source_sql = concat(v_source_sql,char(34),v_dataset_code,char(34),',');

    set v_target_sql = concat('insert into ',v_target_table,' (scheduler_gid,recon_code,dataset_code,tranbrkp_dataset_code,');
  end if;

  -- get dataset recon header
  call pr_get_recondatasetheader(in_recon_code,v_dataset_code,@out_dataset_field_all,@out_recon_field_all);

  set v_source_sql = concat(v_source_sql,@out_dataset_field_all,' from ',v_dataset_table_name);
  set v_source_sql = concat(v_source_sql,' where scheduler_gid = ');
  set v_source_sql = concat(v_source_sql,cast(in_scheduler_gid as nchar));
  set v_source_sql = concat(v_source_sql,v_recon_condition);
  set v_source_sql = concat(v_source_sql,' and delete_flag = ''N''');

  set v_target_sql = concat(v_target_sql,@out_recon_field_all,') ');

  set v_sql = concat(v_target_sql,v_source_sql);

  -- transfer data only for recon mapped fields
  if @out_dataset_field_all <> '' and v_target_table <> '' then
    call pr_run_sql2(v_sql,@msg,@result);

    /*
    set @v_sql = v_sql;
    prepare _sql from @v_sql;
    execute _sql;
    deallocate prepare _sql;
    */
  end if;

  -- insert dummy record to retail auto_increment
  if v_target_table <> '' then
    set v_sql = concat('insert into ',v_target_table,' (scheduler_gid) select ',cast(in_scheduler_gid as nchar));

    call pr_run_sql2(v_sql,@msg,@result);
    /*
    set @v_sql = v_sql;
    prepare _sql from @v_sql;
    execute _sql;
    deallocate prepare _sql;
    */
  end if;

  if (v_recontype_code = 'W' or v_recontype_code = 'B' or v_recontype_code = 'I')
    and (v_dataset_type = 'B' or v_dataset_type = 'T' or v_dataset_type = 'S') then
    -- update tran value
    set v_sql = concat('update ', v_target_table ,' set ');
    set v_sql = concat(v_sql,' tran_value = value_debit + value_credit,');
    set v_sql = concat(v_sql,' excp_value = value_debit + value_credit,');
    set v_sql = concat(v_sql,' tran_mult= if(value_debit > 0,-1,1),');
    set v_sql = concat(v_sql,' tran_acc_mode = if(value_debit > 0,''D'',''C'')');
    set v_sql = concat(v_sql,' where scheduler_gid = ',cast(in_scheduler_gid as nchar));
    set v_sql = concat(v_sql,' and recon_code = ',char(34),in_recon_code,char(34),' ');
    set v_sql = concat(v_sql,' and delete_flag = ''N''');

    call pr_run_sql2(v_sql,@msg2,@result2);

    -- check balance field
    select
      count(*) into v_count
    from recon_mst_treconfield
    where recon_code = in_recon_code
    and recon_field_name in ('bal_value_debit','bal_value_credit')
    and active_status = 'Y'
    and delete_flag = 'N';

    set v_count = ifnull(v_count,0);

    if v_count = 2 and (v_dataset_type = 'B' or v_dataset_type = 'T') then
      -- find the last row for the day to find balance
      set v_sql = concat("
        insert into recon_tmp_ttrangid
        (
          tran_gid,
          tran_date
        )
        select
          max(tran_gid),tran_date
        from  ",v_target_table,"
        where scheduler_gid = ",cast(in_scheduler_gid as nchar),"
        and dataset_code = '",v_dataset_code,"'
        and tran_date is not null
        group by tran_date");

      call pr_run_sql2(v_sql,@msg2,@result2);

      -- insert in temporary table
      set v_sql = concat("
        replace into recon_tmp_tbalance
        (
          tran_date,
          dataset_code,
          bal_value_debit,
          bal_value_credit
        )
        select
          a.tran_date,
          a.dataset_code,
          ifnull(a.bal_value_debit,0),
          ifnull(a.bal_value_credit,0)
        from ",v_target_table," as a
        inner join recon_tmp_ttrangid as b on a.tran_gid = b.tran_gid
        where a.scheduler_gid = ",cast(in_scheduler_gid as nchar),"
        and a.dataset_code = '",v_dataset_code,"'
        and a.delete_flag = 'N'");

      call pr_run_sql2(v_sql,@msg2,@result2);

      if not exists(select * from recon_trn_taccbal
        where scheduler_gid = in_scheduler_gid
        and dataset_code = v_dataset_code
        and delete_flag = 'N') then

        -- update balance
        replace into recon_trn_taccbal
        (
          scheduler_gid,
          dataset_code,
          tran_date,
          bal_value,
          insert_date,
          insert_by
        )
        select
          in_scheduler_gid,
          dataset_code,
          tran_date,
          (bal_value_debit*-1+bal_value_credit),
          sysdate(),
          v_user_code
        from recon_tmp_tbalance;
      end if;
    end if;
  elseif v_recontype_code = 'V' and v_recon_value_flag = 'Y' and v_recon_value_field <> '' then
    -- update tran value
    set v_sql = concat('update ', v_target_table ,' set ');
    set v_sql = concat(v_sql,' tran_value = cast(',v_recon_value_field,' as decimal(15,2)),');
    set v_sql = concat(v_sql,' excp_value = cast(',v_recon_value_field,' as decimal(15,2)),');
    set v_sql = concat(v_sql,' tran_mult= 1,');
    set v_sql = concat(v_sql,' tran_acc_mode = ''V'' ');
    set v_sql = concat(v_sql,' where scheduler_gid = ',cast(in_scheduler_gid as nchar));
    set v_sql = concat(v_sql,' and recon_code = ',char(34),in_recon_code,char(34),' ');
    set v_sql = concat(v_sql,' and delete_flag = ''N''');
  else
    set v_sql = '';
  end if;

  if v_sql <> '' then
    call pr_run_sql2(v_sql,@msg,@result);
    /*
    set @v_sql = v_sql;
    prepare _sql from @v_sql;
    execute _sql;
    deallocate prepare _sql;
    */
  end if;

  -- update job status
  call pr_upd_job(v_job_gid,'C',out_msg,@msg,@result);

  insert into recon_trn_treconscheduler (recon_code,scheduler_gid) select in_recon_code,in_scheduler_gid;

  -- write the ourput status in the file
  set v_sql = 'select ''Status'' as job_status union ';
  set v_sql = concat(v_sql,'select ''',out_msg,''' as job_status ');

  -- call pr_run_rptsql(v_job_gid,v_sql,@msg,@result);

  drop temporary table if exists recon_tmp_ttrangid;
  drop temporary table if exists recon_tmp_tbalance;

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;