﻿DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_tablequery` $$
CREATE PROCEDURE `pr_run_tablequery`
(
  in_reporttemplate_code varchar(32),
  in_recon_code varchar(32),
  in_table_name varchar(128),
  in_condition text,
  in_job_gid int,
  in_outputfile_flag boolean,
  in_outputfile_type varchar(32),
  in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_field_name varchar(128) default '';
  declare v_field_alias_name varchar(128) default '';
  declare v_field_type varchar(32) default '';
  declare v_field text default '';
  declare v_sql_field text default '';
  declare v_sql text default '';
  declare v_static_fields text default '';
  declare v_file_name varchar(128) default '';
  declare v_table_stru_flag boolean default false;
  declare v_rpt_path text default '';
  declare v_report_code text default '';
  declare v_rpt_table_name text default '';
  declare v_recon_field_prefix text default '';
  declare v_sortby_code varchar(32);
  declare v_sorting_field text default '';

  set in_condition = ifnull(in_condition,'');
  set in_job_gid = ifnull(in_job_gid,0);

  drop temporary table if exists recon_tmp_tfield;
  drop temporary table if exists recon_tmp_tfielddisplay;

  create temporary table recon_tmp_tfield
  (
    field_name varchar(255),
    field_alias_name text,
    field_type varchar(32),
    display_order decimal(7,3) not null default 0,
    primary key (field_name),
    key idx_display_order(display_order)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tfielddisplay
  (
    field_name varchar(255),
    display_flag varchar(32),
    display_order decimal(7,3) not null default 0,
    primary key (field_name),
    key idx_display_order(display_order)
  ) ENGINE = MyISAM;

  -- sortby
  select
    report_code,
    sortby_code
  into
    v_report_code,
    v_sortby_code
  from recon_mst_treporttemplate
  where reporttemplate_code = in_reporttemplate_code
  and delete_flag = 'N';

  set v_sortby_code = ifnull(v_sortby_code,'asc');
  set v_report_code = ifnull(v_report_code,'');


  if exists(select * from recon_mst_treporttemplatefield
    where reporttemplate_code = in_reporttemplate_code
    and delete_flag = 'N') then
    set @sno := 0;

    insert into recon_tmp_tfield (field_name,field_alias_name,field_type,display_order)
    select
      a.report_field,
      a.display_desc,
      ifnull(b.recon_field_type,'') as field_type,
      a.display_order
    from recon_mst_treporttemplatefield as a
    left join recon_mst_treconfield as b on a.report_field = b.recon_field_name
      and b.recon_code = in_recon_code
      and b.delete_flag = 'N'
    where a.reporttemplate_code = in_reporttemplate_code
    and a.delete_flag = 'N'
    order by a.display_order;
  elseif exists(select field_name from recon_mst_tsystemfield
    where table_name = in_table_name
    and delete_flag = 'N') then
    set @sno := 0;

    insert into recon_tmp_tfield (field_name,field_alias_name,field_type,display_order)
    select
      field_name,
      fn_get_reconfieldname(in_recon_code,field_name),
      '' as field_type,
      @sno := @sno + 1
    from recon_mst_tsystemfield
    where table_name = in_table_name
    and delete_flag = 'N'
    order by display_order;

    insert ignore into recon_tmp_tfield (field_name,field_alias_name,field_type,display_order)
    select
      a.recon_field_name,
      fn_get_reconfieldname(in_recon_code,a.recon_field_name),
      a.recon_field_type as field_type,
      @sno := @sno + 1
    from recon_mst_treconfield as a
    inner join recon_mst_ttablestru as b on a.recon_field_name = b.field_name
      and b.table_name = in_table_name
      and b.delete_flag = 'N'
    where a.recon_code = in_recon_code
    and a.delete_flag = 'N'
    order by a.display_order;
  elseif exists(select field_name from recon_mst_ttablestru
    where table_name = in_table_name
    and delete_flag = 'N') then

    set @sno := 0;

    insert ignore into recon_tmp_tfield
    (
      field_name,
      field_alias_name,
      field_type,
      display_order
    )
    select
      field_name,
      fn_get_reconfieldname(in_recon_code,field_name),
      '' as field_type,
      @sno := @sno + 1
    from recon_mst_ttablestru
    where table_name = in_table_name
    and display_flag = 'Y'
    and delete_flag = 'N'
    order by display_order;

    insert ignore into recon_tmp_tfield (field_name,field_alias_name,field_type,display_order)
    select
      a.recon_field_name,
      fn_get_reconfieldname(in_recon_code,a.recon_field_name),
      a.recon_field_type as field_type,
      @sno := @sno + 1
    from recon_mst_treconfield as a
    inner join recon_mst_ttablestru as b on a.recon_field_name = b.field_name
      and b.table_name = in_table_name
      and b.display_flag = 'N'
      and b.delete_flag = 'N'
    where a.recon_code = in_recon_code
    and a.delete_flag = 'N'
    order by a.display_order;

    /*
    insert ignore into recon_tmp_tfield (field_name,field_alias_name,field_type)
      select
        t.field_name,
        fn_get_reconfieldname(in_recon_code,t.field_name),
        ifnull(f.recon_field_type,'')
      from recon_mst_ttablestru as t
      left join recon_mst_treconfield as f on t.field_name = f.recon_field_name
        and f.recon_code = in_recon_code
        and f.delete_flag = 'N'
      left join recon_mst_tfieldstru as s on t.field_name = s.field_name and t.delete_flag = 'N'
      where t.table_name = in_table_name
      and (t.display_flag = 'Y' or f.display_flag = 'Y')
      -- and (t.display_flag = 'Y' or (f.display_flag = 'Y' and f.recon_field_name like 'col%'))
      and t.delete_flag = 'N'
      order by if(ifnull(f.display_order,999)>t.display_order,t.display_order,f.display_order);
      -- order by if(t.field_name like 'col%',ifnull(f.display_order,128),t.display_order);
    */

    set v_table_stru_flag := true;
  else
    insert into recon_tmp_tfield (field_name,field_alias_name,field_type)
      SELECT
        t.COLUMN_NAME as field_name,
        ifnull(f.recon_field_name,t.COLUMN_NAME),
        ifnull(f.recon_field_type,'')
      FROM information_schema.columns as t
      left join recon_mst_treconfield as f on t.COLUMN_NAME = f.recon_field_name
        and f.recon_code = in_recon_code
        and f.display_flag = 'Y'
        and f.recon_field_name like 'col%'
        and f.delete_flag = 'N'
      WHERE t.table_schema=database()
      AND t.table_name = in_table_name;

    set v_table_stru_flag := false;
  end if;

  -- display table
  if exists(select * from recon_mst_treporttemplatefield
    where reporttemplate_code = in_reporttemplate_code
    and delete_flag = 'N') then
    -- get report table name
    select
      table_name,
      recon_field_prefix
    into
      v_rpt_table_name,
      v_recon_field_prefix
    from recon_mst_treport
    where report_code = v_report_code
    and delete_flag = 'N';

    set v_recon_field_prefix = ifnull(v_recon_field_prefix,'');

    insert ignore into recon_tmp_tfielddisplay
    (
      field_name,
      display_flag,
      display_order
    )
    select
      ifnull(b.field_name,replace(a.report_field,v_recon_field_prefix,'')) as field_name,
      a.display_flag,
      a.display_order
    from recon_mst_treporttemplatefield as a
    left join recon_mst_tsystemfield as b on b.report_field_name = a.report_field
      and b.table_name = in_table_name
      and b.delete_flag = 'N'
    where a.reporttemplate_code = in_reporttemplate_code
    and a.active_status = 'Y'
    and a.display_flag = 'Y'
    and a.delete_flag = 'N'
    order by a.display_order;

    -- sort order
    select
      group_concat(ifnull(b.field_name,if(instr(a.report_field,'.') = 0,a.report_field,SPLIT(a.report_field,'.',2))))
    into
      v_sorting_field
    from recon_mst_treporttemplatesorting as a
    left join recon_mst_tsystemfield as b on b.report_field_name = a.report_field
      and b.table_name = in_table_name
      and b.delete_flag = 'N'
    where a.reporttemplate_code = in_reporttemplate_code
    and a.active_status = 'Y'
    and a.delete_flag = 'N'
    order by a.sorting_order;

    set v_sorting_field = ifnull(v_sorting_field,'');
  else
    insert ignore into recon_tmp_tfielddisplay
    (
      field_name,
      display_flag,
      display_order
    )
    select field_name,'Y',display_order from recon_tmp_tfield order by display_order;
  end if;

  field_block:begin
    declare field_done int default 0;
    declare field_cursor cursor for
      select a.field_name,a.field_alias_name,a.field_type from recon_tmp_tfield as a
      inner join recon_tmp_tfielddisplay as b on a.field_name = b.field_name
      where b.display_flag = 'Y'
      order by b.display_order;

    declare continue handler for not found set field_done=1;

    open field_cursor;

    field_loop: loop
      fetch field_cursor into v_field_name,v_field_alias_name,v_field_type;
      if field_done = 1 then leave field_loop; end if;

      set v_field = concat(fn_get_fieldformat(in_recon_code,v_field_name),' as ',char(39),v_field_alias_name,char(39));

      /*
      if v_field_type = 'NUMBER' then
        set v_field = concat('ifnull(',fn_get_fieldformat(in_recon_code,v_field_name),',0) as ',char(39),v_field_alias_name,char(39));
      else
        set v_field = concat('ifnull(cast(',fn_get_fieldformat(in_recon_code,v_field_name),' as nchar),',char(39),char(39),') as ',char(39),v_field_alias_name,char(39));
      end if;
      */

      if v_sql_field = '' then
        set v_sql_field = v_field;
      else
        set v_sql_field = concat(v_sql_field,',',v_field);
      end if;

      if v_static_fields = '' then
        set v_static_fields = concat('"',v_field_alias_name,'"');
      else
        set v_static_fields = concat(v_static_fields,',"',v_field_alias_name,'"');
      end if;
    end loop field_loop;

    if (v_sql_field = '') then
      set v_static_fields := '';
      set v_sql_field = '*';
    else
      if in_job_gid > 0 and in_outputfile_flag = true and in_outputfile_type = 'csv' then
        set v_static_fields= concat('Select ',v_static_fields,' union all ');
      else
        set v_static_fields := '';
      end if;
    end if;

    set v_sql = concat(v_static_fields,'select a.* from (');
    set v_sql = concat(v_sql,'select ',v_sql_field,' from ',in_table_name,' where 1=1 ',in_condition);

    /*
    -- add sorting order
    if v_sorting_field <> '' then
      set v_sql = concat(v_sql,' order by ',v_sorting_field,' ',v_sortby_code,' ');
    end if;
    */

    set v_sql = concat(v_sql,') as a ');

    if in_job_gid > 0 and in_outputfile_flag = true and in_outputfile_type = 'csv' then
      set v_rpt_path = fn_get_configvalue('mysql_rpt_path');

      set @outfile_qry = concat(" INTO outfile '",v_rpt_path,cast(in_job_gid as nchar),".csv'
						  FIELDS TERMINATED BY ','
              OPTIONALLY ENCLOSED BY '""'
						  LINES TERMINATED BY '\n' ;");

      set v_sql = concat(v_sql,@outfile_qry);

      call pr_upd_job(in_job_gid,'P','Inprogress',@msg,@result);
    end if;

	  call pr_run_sql(v_sql,@msg,@result);

    if in_job_gid > 0 then
      call pr_upd_job(in_job_gid,'C','Completed',@msg,@result);
    end if;

    drop temporary table if exists recon_tmp_tfield;
    drop temporary table if exists recon_tmp_tfielddisplay;

    set out_msg = @msg;
    set out_result = @result;

  end field_block;
end $$

DELIMITER ;