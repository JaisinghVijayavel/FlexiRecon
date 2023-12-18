DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_tablequery` $$
CREATE PROCEDURE `pr_get_tablequery`
(
  in_recon_code varchar(32),
  in_table_name varchar(128),
  in_condition text,
  in_job_gid int,
  in_user_code varchar(50),
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

  set in_condition = ifnull(in_condition,'');
  set in_job_gid = ifnull(in_job_gid,0);

  drop temporary table if exists recon_tmp_tfield;
  create temporary table recon_tmp_tfield
  (
    field_gid int unsigned NOT NULL AUTO_INCREMENT,
    field_name varchar(255),
    field_alias_name text,
    field_type varchar(32),
    primary key (field_gid),
    key idx_field_name(field_name)
  ) ENGINE = MyISAM;

  if exists(select field_name from recon_mst_ttablestru
    where table_name = in_table_name
    and delete_flag = 'N') then

    insert into recon_tmp_tfield (field_name,field_alias_name,field_type)
      select
        t.field_name,
        ifnull(f.recon_field_desc,ifnull(s.field_alias_name,t.field_name)),
        ifnull(f.recon_field_type,'')
      from recon_mst_ttablestru as t
      left join recon_mst_treconfield as f on t.field_name = f.recon_field_name
        and f.recon_code = in_recon_code
        and f.delete_flag = 'N'
      left join recon_mst_tfieldstru as s on t.field_name = s.field_name and t.delete_flag = 'N'
      where t.table_name = in_table_name
      and (t.display_flag = 'Y' or (f.display_flag = 'Y' and f.recon_field_name like 'col%'))
      and t.delete_flag = 'N'
      order by if(t.field_name like 'col%',ifnull(f.display_order,128),t.display_order);

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

  field_block:begin
    declare field_done int default 0;
    declare field_cursor cursor for
      select field_name,field_alias_name,field_type from recon_tmp_tfield;

    declare continue handler for not found set field_done=1;

    open field_cursor;

    field_loop: loop
      fetch field_cursor into v_field_name,v_field_alias_name,v_field_type;
      if field_done = 1 then leave field_loop; end if;

      if v_field_type = 'NUMBER' then
        set v_field = concat('ifnull(',fn_get_fieldformat(in_recon_code,v_field_name),',0) as ',char(39),v_field_alias_name,char(39));
      else
        set v_field = concat('ifnull(cast(',fn_get_fieldformat(in_recon_code,v_field_name),' as nchar),',char(39),char(39),') as ',char(39),v_field_alias_name,char(39));
      end if;

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
      if in_job_gid > 0 then
        set v_static_fields= concat('Select ',v_static_fields,' union all ');
      else
        set v_static_fields := '';
      end if;
    end if;

    set v_sql = concat(v_static_fields,'select ',v_sql_field,' from ',in_table_name,' where 1=1 ',in_condition);

    if in_job_gid > 0 then
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
    
    set out_msg = @msg;
    set out_result = @result;
    
  end field_block;
end $$

DELIMITER ;