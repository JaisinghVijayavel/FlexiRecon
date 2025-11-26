DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_reportparam` $$
CREATE PROCEDURE `pr_get_reportparam`(
  in_report_code varchar(32),
  in_recon_code varchar(32)
)
BEGIN
    declare v_src_table_name text default '';
    declare v_rpt_table_name text default '';
    declare v_recon_field_prefix text default '';
    declare v_ds_db_name text default '';

    drop temporary table if exists recon_tmp_treportparam;

    CREATE TEMPORARY TABLE recon_tmp_treportparam
    (
      report_code varchar(32) not null,
      reportparam_code varchar(255) not null,
      reportparam_desc varchar(255) not null,
      reportparam_order double(7,3) not null default 0,
      PRIMARY KEY (report_code,reportparam_code),
      key idx_reportparam_order(reportparam_order)
    ) ENGINE = MyISAM;

    
    select
      table_name,
      src_table_name,
      recon_field_prefix
    into
      v_rpt_table_name,
      v_src_table_name,
      v_recon_field_prefix
    from recon_mst_treport
    where report_code = in_report_code
    and delete_flag = 'N';

    set v_rpt_table_name = ifnull(v_rpt_table_name,'');
    set v_src_table_name = ifnull(v_src_table_name,'');
    set v_recon_field_prefix = ifnull(v_recon_field_prefix,'');

    if exists(select recon_flag from recon_mst_treport
      where report_code = in_report_code
      and recon_flag = 'Y'
      and delete_flag = 'N') then

      set @sno := 0;

      insert ignore into recon_tmp_treportparam
      (
        report_code,
        reportparam_code,
        reportparam_desc,
        reportparam_order
      )
      SELECT
        in_report_code,
        report_field_name,
		-- report_field_desc,
        concat( report_field_desc, '-', fn_get_fieldtype(in_recon_code, report_field_name)),
        @sno := @sno + 1

      FROM recon_mst_tsystemfield
      WHERE table_name = v_rpt_table_name
      and active_status = 'Y'
      and delete_flag = 'N'
      order by display_order;

      insert ignore into recon_tmp_treportparam
      (
        report_code,
        reportparam_code,
        reportparam_desc,
        reportparam_order
      )
      select
        in_report_code as report_code,
        concat(v_recon_field_prefix,recon_field_name) as reportparam_code,
        case when recon_field_type is null then fn_get_reconfieldname(recon_code,recon_field_name)
        else concat(fn_get_reconfieldname(recon_code,recon_field_name), '-', ifnull(recon_field_type,'')) end
         as reportparam_desc,
        @sno := @sno + 1
      from recon_mst_treconfield
      where recon_code = in_recon_code
      and active_status = 'Y'
      and delete_flag = 'N'
      order by display_order;
    elseif exists(select recon_flag from recon_mst_treport
      where report_code = in_report_code
      and report_exec_type = 'D'
      and delete_flag = 'N') then
      insert ignore into recon_tmp_treportparam
      (
        report_code,
        reportparam_code,
        reportparam_desc,
        reportparam_order
      )
      SELECT
        in_report_code,
        dataset_table_field,
        concat( field_name,'-', ifnull(field_type,'')),
        @sno := @sno + 1
      FROM recon_mst_tdatasetfield
      WHERE dataset_code = in_report_code
      and active_status = 'Y'
      and delete_flag = 'N'
      order by dataset_seqno;

      set v_ds_db_name = fn_get_configvalue('dataset_db_name');

      if v_ds_db_name = '' then
        select database() into v_ds_db_name;
      end if;

      insert ignore into recon_tmp_treportparam
      (
        report_code,
        reportparam_code,
        reportparam_desc,
        reportparam_order
      )
      select
        in_report_code,'scheduler_gid','Scheduler Id - INTEGER',@sno := @sno + 1
      from INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_NAME = in_report_code
      AND TABLE_SCHEMA = v_ds_db_name
      and COLUMN_NAME = 'scheduler_gid';
    else
      insert into recon_tmp_treportparam
      (
        report_code,
        reportparam_code,
        reportparam_desc,
        reportparam_order
      )
      select a.* from
      (
        SELECT
	        in_report_code as report_code,
          report_field_name as reportparam_code,
          -- report_field_desc as reportparam_desc,
           concat(report_field_desc, '-', fn_get_fieldtype(in_recon_code, report_field_name)) as reportparam_desc,
          display_order as reportparam_order
        FROM recon_mst_tsystemfield
        WHERE table_name = v_rpt_table_name
        and active_status = 'Y'
        and delete_flag = 'N'
      ) as a;
    end if;

    select * from recon_tmp_treportparam order by reportparam_order;

    drop temporary table if exists recon_tmp_treportparam;
END $$

DELIMITER ;