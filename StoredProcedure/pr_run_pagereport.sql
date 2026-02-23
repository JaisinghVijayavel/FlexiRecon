DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_pagereport` $$
CREATE PROCEDURE `pr_run_pagereport`(
  in in_archival_code varchar(32),
  in in_reporttemplate_code varchar(32),
  in in_reporttemplateresultset_code varchar(32),
  in in_recon_code varchar(32),
  in in_report_code varchar(32),
  in in_report_condition text,
  in in_ip_addr varchar(255),
  in in_user_code varchar(32),
  out out_rec_count int,
  out out_msg text,
  out out_result int
)
me:BEGIN
  /*
    Created By : Vijayavel
    Created Date :

    Updated By : Vijayavel
    updated Date : 18-02-2026

    Version : 4
  */

  declare v_recon_code varchar(32) default '';
  declare v_report_code varchar(32) default '';
  declare v_reporttemplateresultset_code varchar(32) default '';
  declare v_sortby_code varchar(32);

  declare v_rptsession_gid int default 0;
  declare v_report_desc text default '';
  declare v_report_exec_type text default '';
  declare v_sp_name text default '';
  declare v_table_name text default '';
  declare v_src_table_name text default '';
  declare v_rpt_table_name text default '';
  declare v_sql text default '';
  declare v_recon_code_field text default '';
  declare v_recon_flag text default '';
  declare v_multi_recon_flag text default '';
  declare v_report_default_condition text default '';
  declare v_sorting_order text default '';
  declare v_dataset_db_name text default '';

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  set in_archival_code = ifnull(in_archival_code,'');
  set in_reporttemplate_code = ifnull(in_reporttemplate_code,'');

  if in_reporttemplateresultset_code = '' then
    set in_reporttemplateresultset_code = null;
  end if;

  -- get report and recon code
  if in_reporttemplate_code <> '' then
    -- get report code
    select
      a.recon_code,
      b.reporttemplateresultset_code,
      b.src_report_code
    into
      v_recon_code,
      v_reporttemplateresultset_code,
      v_report_code
    from recon_mst_treporttemplate as a
    inner join recon_mst_treporttemplateresultset as b on a.reporttemplate_code = b.reporttemplate_code
      and b.delete_flag = 'N'
    where a.reporttemplate_code = in_reporttemplate_code
    and b.reporttemplateresultset_code = ifnull(in_reporttemplateresultset_code,reporttemplateresultset_code)
    and a.delete_flag = 'N'
    order by b.resultset_order limit 1;

    set v_recon_code = ifnull(v_recon_code,'');
    set v_report_code = ifnull(v_report_code,'');
    set v_reporttemplateresultset_code = ifnull(v_reporttemplateresultset_code,'');

    -- get sorting field
    select
      group_concat(concat(report_field,' ',sorting_type))
    into
      v_sortby_code
    from recon_mst_treporttemplatesorting
    where reporttemplate_code = in_reporttemplate_code
    and reporttemplateresultset_code = v_reporttemplateresultset_code
    order by sorting_order;

    set v_sortby_code = lower(ifnull(v_sortby_code,'asc'));
  else
    set v_recon_code = ifnull(in_recon_code,'');
    set v_report_code = ifnull(in_report_code,'');
    set v_sortby_code = 'asc';
  end if;

  if exists(select report_desc from recon_mst_treport
     where report_code = v_report_code
     and delete_flag = 'N') then
    select
      report_code,
      report_desc,
      report_exec_type,
      table_name,
      src_table_name,
      rpt_table_name,
      sp_name,
      recon_code_field,
      default_condition,
      recon_flag,
      multi_recon_flag
    into
      v_report_code,
      v_report_desc,
      v_report_exec_type,
      v_table_name,
      v_src_table_name,
      v_rpt_table_name,
      v_sp_name,
      v_recon_code_field,
      v_report_default_condition,
      v_recon_flag,
      v_multi_recon_flag
    from recon_mst_treport
    where report_code = v_report_code
    and delete_flag = 'N';
  else
    set out_msg = 'Invalid report';
    set out_result = 0;

    leave me;
  end if;

  set v_report_code = ifnull(v_report_code,'');
  set v_report_desc = ifnull(v_report_desc,v_report_code);
  set v_report_exec_type = ifnull(v_report_exec_type,'');

  set v_table_name = ifnull(v_table_name,'');
  set v_src_table_name = ifnull(v_src_table_name,'');
  set v_rpt_table_name = ifnull(v_rpt_table_name,'');

  set v_sp_name = ifnull(v_sp_name,'');
  set v_recon_code_field = ifnull(v_recon_code_field,'');
  set v_report_default_condition = ifnull(v_report_default_condition,'');

  set v_recon_flag = ifnull(v_recon_flag,'N');
  set v_multi_recon_flag = ifnull(v_multi_recon_flag,'N');

  if v_rpt_table_name <> '' then
    set v_table_name = v_rpt_table_name;
  end if;

  if v_table_name = '' and v_report_exec_type <> 'D' then
    set out_msg = 'Invalid table name';
    set out_result = 0;

    leave me;
  end if;

  set in_report_condition = ifnull(in_report_condition,'');

  if v_recon_code_field <> '' and v_recon_flag = 'Y' and v_multi_recon_flag = 'N' then
    set in_report_condition = concat(' and ',v_recon_code_field,' = ',char(34),v_recon_code,char(34),' ', in_report_condition);
  end if;

  set in_report_condition = concat(in_report_condition,' ',v_report_default_condition);

  -- sorting order
  if in_reporttemplate_code <> '' then
    select
      group_concat(report_field)
    into
      v_sorting_order
    from recon_mst_treporttemplatesorting
    where reporttemplate_code = in_reporttemplate_code
    and active_status = 'Y'
    and delete_flag = 'N'
    order by sorting_order;
  else
    select
      group_concat(report_field)
    into
      v_sorting_order
    from recon_mst_treportsorting
    where report_code = v_report_code
    and active_status = 'Y'
    and delete_flag = 'N'
    order by sorting_order;
  end if;

  set v_sorting_order = ifnull(v_sorting_order,'');

  if v_sorting_order <> '' then
    set v_sorting_order = concat('order by ',v_sorting_order,' ',v_sortby_code);
  end if;

  -- create new report session
  insert into recon_trn_treportsession (report_code,ip_addr,insert_date,insert_by)
    select v_report_code,in_ip_addr,sysdate(),in_user_code;

  select last_insert_id() into v_rptsession_gid;

  if v_report_exec_type = 'S' then
    if v_rptsession_gid > 0 then
      set v_sql = concat('delete from ',v_table_name,' where rptsession_gid in (');
      set v_sql = concat(v_sql,'select rptsession_gid from recon_trn_treportsession ');
      set v_sql = concat(v_sql,'where rptsession_gid < ',cast(v_rptsession_gid as nchar),') ');
      set v_sql = concat(v_sql,'and rptsession_gid > 0 ');
      set v_sql = concat(v_sql,'and user_code = ',char(39),in_user_code,char(39));

      call pr_run_sql(v_sql,@msg,@result);
    end if;

    call pr_run_sp(in_archival_code,v_recon_code,v_sp_name,0,v_rptsession_gid,in_report_condition,v_sorting_order,in_user_code,@msg,@result);

    set v_sql = concat('select count(*) into @rec_count from ',v_table_name,' ');
    set v_sql = concat(v_sql,'where rptsession_gid = ',cast(v_rptsession_gid as nchar),' ');
    set v_sql = concat(v_sql,'LOCK IN SHARE MODE');

    call pr_run_sql(v_sql,@msg,@result);
  elseif v_report_exec_type = 'D' then
    set v_dataset_db_name = fn_get_configvalue('dataset_db_name');

    if v_dataset_db_name <> '' then
      set v_table_name = concat(v_dataset_db_name,'.',v_report_code);
    else
      set v_table_name = v_report_code;
    end if;

    set v_sql = concat('select count(*) into @rec_count from ',v_table_name,' ');
    set v_sql = concat(v_sql,'where true ',in_report_condition);
    set v_sql = concat(v_sql,'LOCK IN SHARE MODE');

    call pr_run_sql(v_sql,@msg,@result);
  elseif v_report_exec_type = 'T' then
    set v_sql = concat('select count(*) into @rec_count from ',v_table_name,' ');
    set v_sql = concat(v_sql,'where true ',in_report_condition);
    set v_sql = concat(v_sql,'LOCK IN SHARE MODE');

    call pr_run_sql(v_sql,@msg,@result);
  end if;

  set out_rec_count = ifnull(@rec_count,0);
  set out_msg = concat(v_report_desc,' generation initiated in the report session id ',cast(v_rptsession_gid as nchar));
  set out_result = v_rptsession_gid;

  /*
  if in_resultset_flag = true then
    -- if v_rptsession_gid > 0 then
    --   set v_resultset_condition = concat(' and rptsession_gid = ',cast(v_rptsession_gid as unsigned),' ');
    -- end if;

    -- call pr_get_tablequery(v_table_name,v_resultset_condition,0,in_user_code,@msg,@result);

    call pr_get_pagenoreport(in_rptsession_gid,in_report_code,1,in_page_size,out_rec_count,@msg,@result);
  end if;
  */
end $$

DELIMITER ;