DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_treporttemplate` $$
CREATE PROCEDURE `pr_recon_mst_treporttemplate`
(
	inout in_reporttemplate_gid int(10),
	inout in_reporttemplate_code varchar(32),
	in in_reporttemplate_name varchar(255),
	in in_report_code varchar(32),
  in in_recon_code varchar(32),
  in in_sortby_code varchar(32),
	in in_action varchar(32),
	in in_system_flag varchar(32),
	in in_active_status char(1),
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32),
	out out_msg text,
	out out_result int
)
me:BEGIN
	/*
		Created By : Hema
    Created Date : 13-02-2024

    Updated By : Vijayavel
    Updated Date : 07-04-2024

    Version No : 1
  */
	declare err_msg text default '';
	declare err_flag boolean default false;
	declare v_msg text default '';
	declare v_reporttemplate_gid int default 0;
  declare v_rpt_table_name text default '';

	if(in_action = 'INSERT' or in_action = 'UPDATE') then
		if in_reporttemplate_name = '' or in_reporttemplate_name is null then
			set err_msg := concat(err_msg,'Report templetename cannot be empty,');
			set err_flag := true;
		end if;

     if in_active_status <> 'Y' and
				in_active_status <> 'N' and
				in_active_status <> 'D' or
				in_active_status is null then
			set err_msg := concat(err_msg,'Invalid active status value,');
			set err_flag := true;
		end if;

    if not exists (select report_gid from recon_mst_treport
      where report_code = in_report_code
      and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Invalid report code,');
			set err_flag := true;
		end if;
	end if;

	if (in_action = 'INSERT') then
    -- get rpt_table_name
    select
      table_name
    into
      v_rpt_table_name
    from recon_mst_treport
    where report_code = in_report_code
    and delete_flag = 'N';

    set v_rpt_table_name = ifnull(v_rpt_table_name,'');

    -- template variable
		set in_reporttemplate_code = fn_get_autocode('RT');
		set in_reporttemplate_gid = 0;

		insert into recon_mst_treporttemplate
		(
			reporttemplate_gid,
			reporttemplate_code,
			reporttemplate_name,
			report_code,
      recon_code,
      sortby_code,
			system_flag,
			active_status,
			insert_date,
			insert_by
		)
		value
		(
			in_reporttemplate_gid,
			in_reporttemplate_code,
			in_reporttemplate_name,
			in_report_code,
      in_recon_code,
      in_sortby_code,
			in_system_flag,
			'Y',
			sysdate(),
			in_user_code
		);

		select max(reporttemplate_gid) into v_reporttemplate_gid from recon_mst_treporttemplate;

		set in_reporttemplate_gid = v_reporttemplate_gid;

    -- insert in recon_mst_treporttemplatefilter
    insert into recon_mst_treporttemplatefilter
    (
      reporttemplate_code,
      filter_seqno,
      report_field,
      filter_criteria,
      filter_value,
      open_parentheses_flag,
      close_parentheses_flag,
      join_condition,
      system_flag,
      active_status,
      insert_date,
      insert_by
    )
    select
      in_reporttemplate_code,
      filter_seqno,
      report_field,
      filter_criteria,
      filter_value,
      open_parentheses_flag,
      close_parentheses_flag,
      join_condition,
      'Y',
      active_status,
      sysdate(),
      in_user_code
    from recon_mst_treportfilter
    where report_code = in_report_code
    and active_status = 'Y'
    and delete_flag = 'N'
    order by filter_seqno;

    -- insert in recon_mst_treporttemplatesorting
    insert into recon_mst_treporttemplatesorting
    (
      reporttemplate_code,
      report_field,
      sorting_order,
      active_status,
      insert_date,
      insert_by
    )
    select
      in_reporttemplate_code,
      report_field,
      sorting_order,
      active_status,
      sysdate(),
      in_user_code
    from recon_mst_treportsorting
    where report_code = in_report_code
    and active_status = 'Y'
    and delete_flag = 'N'
    order by sorting_order;

    -- insert in recon_mst_treporttemplatefield
    set @sno := 0;

		insert into recon_mst_treporttemplatefield
		(
			reporttemplate_code,
			report_field,
			display_desc,
			display_flag,
			display_order,
			system_flag,
			active_status,
			insert_date,
			insert_by
		)
		SELECT
			in_reporttemplate_code,
			field_name,
			report_field_desc,
			'Y',
			@sno := @sno + 1,
			'Y',
      'Y',
      sysdate(),
      in_user_code
		FROM recon_mst_tsystemfield
		WHERE table_name = v_rpt_table_name
		and active_status = 'Y'
		and delete_flag = 'N'
		order by display_order;

		insert ignore into recon_mst_treporttemplatefield
		(
			reporttemplate_code,
			report_field,
			display_desc,
			display_flag,
			display_order,
			system_flag,
			active_status,
			insert_date,
			insert_by
		)
		SELECT
			in_reporttemplate_code,
      recon_field_name,
      fn_get_reconfieldname(recon_code,recon_field_name),
			'Y',
			@sno := @sno + 1,
			'N',
      'Y',
      sysdate(),
      in_user_code
    from recon_mst_treconfield
    where recon_code = in_recon_code
    and active_status = 'Y'
    and delete_flag = 'N'
    order by display_order;

		set out_result = 1;
		set out_msg = 'Record saved successfully.. !';
	elseif(in_action = 'UPDATE') then
		update recon_mst_treporttemplate set
			reporttemplate_name = in_reporttemplate_name,
			report_code = in_report_code,
			recon_code = in_recon_code,
      sortby_code = in_sortby_code,
			system_flag = in_system_flag,
			active_status = in_active_status,
			update_date = sysdate(),
			update_by = in_user_code
		where reporttemplate_gid = in_reporttemplate_gid
		and delete_flag = 'N';

		set out_result = 1;
		set out_msg = 'Record Updated Successfully.. !';
	elseif(in_action = 'DELETE') then
		update recon_mst_treporttemplate set
			active_status = 'N',
			update_date = sysdate(),
			update_by = in_user_code,
      delete_flag = 'Y'
		where reporttemplate_gid = in_reporttemplate_gid
    and delete_flag = 'N';

		set out_result = 1;
		set out_msg = 'Record deleted successfully.. !';
	end if;
END $$

DELIMITER ;