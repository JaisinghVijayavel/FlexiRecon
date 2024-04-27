DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_reporttemplatefield` $$
CREATE PROCEDURE `pr_get_reporttemplatefield`
(
  in_reporttemplate_code varchar(32),
  in_recon_code varchar(32),
  in_report_code varchar(32)
)
BEGIN
    declare v_report_code text default '';
    declare v_recon_code text default '';
    declare v_recon_flag text default '';
    declare v_src_table_name text default '';
    declare v_rpt_table_name text default '';
    declare v_recon_field_prefix text default '';

    set in_reporttemplate_code = ifnull(in_reporttemplate_code,'');

    -- get recon and report code
    select
      report_code,
      recon_code
    into
      v_report_code,
      v_recon_code
    from recon_mst_treporttemplate
    where reporttemplate_code = in_reporttemplate_code
    and delete_flag = 'N';

    set v_report_code = ifnull(v_report_code,'');
    set v_recon_code = ifnull(v_recon_code,'');

    if in_reporttemplate_code = '' then
      set v_recon_code = in_recon_code;
      set v_report_code = in_report_code;
    end if;

    if exists(select * from recon_mst_treporttemplatefield
      where reporttemplate_code = in_reporttemplate_code
      and delete_flag = 'N') then
      select
        report_field,
        fn_get_reconfieldname(v_recon_code,report_field) as report_field_desc,
        display_desc,
        display_flag,
        display_order,
        system_flag
      from recon_mst_treporttemplatefield
      where reporttemplate_code = in_reporttemplate_code
      and active_status = 'Y'
      and delete_flag = 'N'
      order by display_order;
    else
			drop temporary table if exists recon_tmp_treportparam;

			CREATE TEMPORARY TABLE recon_tmp_treportparam
			(
				report_code varchar(32) not null,
				reportparam_code varchar(32) not null,
				reportparam_desc varchar(255) not null,
        display_flag varchar(32) not null default 'N',
				reportparam_order double(7,3) not null default 0,
        system_flag varchar(32) not null default 'N',
				PRIMARY KEY (report_code,reportparam_code),
				key idx_reportparam_order(reportparam_order)
			) ENGINE = MyISAM;

      -- get src_table_name
      select
        table_name,
        src_table_name,
        recon_field_prefix,
        recon_flag
      into
        v_rpt_table_name,
        v_src_table_name,
        v_recon_field_prefix,
        v_recon_flag
      from recon_mst_treport
      where report_code = v_report_code
      and delete_flag = 'N';

      set v_rpt_table_name = ifnull(v_rpt_table_name,'');
      set v_src_table_name = ifnull(v_src_table_name,'');
      set v_recon_field_prefix = ifnull(v_recon_field_prefix,'');
      set v_recon_flag = ifnull(v_recon_flag,'');

      set @sno := 0;

      insert into recon_tmp_treportparam
      (
        report_code,
        reportparam_code,
        reportparam_desc,
        reportparam_order,
        system_flag
      )
      SELECT
        v_report_code,
        field_name,
        report_field_desc,
        @sno := @sno + 1,
        system_flag
      FROM recon_mst_tsystemfield
      WHERE table_name = v_rpt_table_name
      and active_status = 'Y'
      and delete_flag = 'N'
      order by display_order;

      if v_recon_flag = 'Y' then
				insert ignore into recon_tmp_treportparam
				(
					report_code,
					reportparam_code,
					reportparam_desc,
					reportparam_order
				)
				select
					v_report_code as report_code,
					recon_field_name as reportparam_code,
					fn_get_reconfieldname(recon_code,recon_field_name),
					@sno := @sno + 1
				from recon_mst_treconfield
				where recon_code = v_recon_code
				and active_status = 'Y'
				and delete_flag = 'N'
				order by display_order;
      end if;

			select
				reportparam_code as report_field,
				reportparam_desc as report_field_desc,
				reportparam_desc as display_desc,
				display_flag,
				reportparam_order as display_order
			from recon_tmp_treportparam
			order by reportparam_order;

			drop temporary table if exists recon_tmp_treportparam;
    end if;
END $$

DELIMITER ;