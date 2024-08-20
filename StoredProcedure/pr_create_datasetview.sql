DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_create_datasetview` $$
CREATE PROCEDURE `pr_create_datasetview`
(
  in_dataset_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_field_name varchar(128) default '';
  declare v_field_alias_name varchar(128) default '';
  declare v_field_type varchar(128) default '';
  declare v_field_length varchar(128) default '';
  declare v_field text default '';
  declare v_sql_field text default '';
  declare v_sql text default '';
  declare v_static_fields text default '';
  declare v_file_name varchar(128) default '';
  declare v_table_stru_flag boolean default false;

  declare v_dataset_db_name text default '';
  declare v_table_name text default '';
  declare v_view_name text default '';
  declare v_recon_code text default '';

  drop temporary table if exists recon_tmp_tfield;
  drop temporary table if exists recon_tmp_tfielddisplay;

  -- drop table if exists recon_tmp_tfield;

  create temporary table recon_tmp_tfield
  (
    field_name varchar(255),
    field_alias_name text,
    field_type varchar(32),
    field_length varchar(32),
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

    set v_dataset_db_name = fn_get_configvalue('dataset_db_name');

    if v_dataset_db_name <> '' then
      set v_table_name = concat(v_dataset_db_name,'.',in_dataset_code);
    else
      set v_table_name = in_dataset_code;
    end if;

    insert into recon_tmp_tfield (field_name,field_alias_name,field_type,field_length,display_order)
    select
      dataset_table_field,
      field_name,
      field_type,
      field_length,
      dataset_field_sno
    from recon_mst_tdatasetfield
    where dataset_code = in_dataset_code
    and delete_flag = 'N'
    order by dataset_field_sno;

  -- display table
    insert ignore into recon_tmp_tfielddisplay
    (
      field_name,
      display_flag,
      display_order
    )
    select field_name,'Y',display_order from recon_tmp_tfield order by display_order;

  field_block:begin
    declare field_done int default 0;
    declare field_cursor cursor for
      select a.field_name,a.field_alias_name,a.field_type,a.field_length from recon_tmp_tfield as a
      inner join recon_tmp_tfielddisplay as b on a.field_name = b.field_name
      where b.display_flag = 'Y'
      order by b.display_order;

    declare continue handler for not found set field_done=1;

    open field_cursor;

    field_loop: loop
      fetch field_cursor into v_field_name,v_field_alias_name,v_field_type,v_field_length;
      if field_done = 1 then leave field_loop; end if;

      set v_field = concat(fn_get_fieldtypecast(v_field_name,v_field_type,v_field_length),' as ',char(39),v_field_alias_name,char(39));

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
  end field_block;

  drop temporary table if exists recon_tmp_tfield;
  drop temporary table if exists recon_tmp_tfielddisplay;

  -- set view name
  set v_view_name = concat(in_dataset_code,'_view');

  set v_sql = concat('drop view if exists ',v_view_name);
  call pr_run_sql(v_sql,@msg,@result);

  set v_sql = concat('create view ',v_view_name,' as select a.* from (');
  set v_sql = concat(v_sql,'select ',v_sql_field,' from ',v_table_name,' where delete_flag = ''N'' ');

  set v_sql = concat(v_sql,') as a ');

  call pr_run_sql(v_sql,@msg,@result);

  set out_msg = @msg;
  set out_result = @result;
end $$

DELIMITER ;