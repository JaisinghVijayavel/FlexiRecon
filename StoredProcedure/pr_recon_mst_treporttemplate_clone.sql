DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_treporttemplate_clone` $$
CREATE PROCEDURE `pr_recon_mst_treporttemplate_clone`
(
	in in_clone_reporttemplate_code varchar(32),
	in in_reporttemplate_name varchar(255),
	in in_report_code varchar(32),
	in in_action varchar(32),
	in in_active_status char(1),
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32),
	out out_msg text,
	out out_result int,
	out out_reporttemplate_code varchar(32)
)
me:BEGIN
	/*
		Created By : Hema
    Created Date : Mar-16-2023

    Updated By : 
    Updated Date :

    Version No : 1
  */
  
	declare err_msg text default '';
	declare err_flag boolean default false;
	declare v_msg text default '';
	declare v_reporttemplate_gid int default 0;
	declare v_reporttemplate_code text default '';

	if (in_action = 'INSERT') then
		set v_reporttemplate_code = fn_get_autocode('RT');
		set v_reporttemplate_gid = 0;
		
		insert into recon_mst_treporttemplate
		(
			reporttemplate_gid, 
			reporttemplate_code, 
			reporttemplate_name, 
			report_code, 
			system_flag,
			active_status, 
			insert_date, 
			insert_by
		) 
		select
			v_reporttemplate_gid, 
			v_reporttemplate_code,
			in_reporttemplate_name, 
			report_code,
			system_flag, 
			active_status, 
			insert_date, 
			insert_by
		from recon_mst_treporttemplate
		where reporttemplate_code = in_clone_reporttemplate_code
		and active_status = 'Y'
		and delete_flag = 'N';

		insert into recon_mst_treporttemplatefilter
		(
			reporttemplatefilter_gid, 
			reporttemplate_code, 
			filter_seqno, 
			report_field, 
			filter_criteria, 
			filter_value, 
			open_parentheses_flag, 
			close_parentheses_flag, 
			join_condition, 
			active_status, 
			insert_date, 
			insert_by
		) 
    select
			0, 
			v_reporttemplate_code, 
			filter_seqno, 
			report_field, 
			filter_criteria, 
			filter_value, 
			open_parentheses_flag, 
			close_parentheses_flag, 
			join_condition, 
			active_status, 
			insert_date, 
			insert_by
    from recon_mst_treporttemplatefilter
    where reporttemplate_code = in_clone_reporttemplate_code
    and active_status = 'Y'
    and delete_flag = 'N';
		
		set v_msg = 'Record Saved Successfully.. !';
		set out_result = 1;
		set out_msg = v_msg; 
		set out_reporttemplate_code = v_reporttemplate_code;
	end if;
END $$

DELIMITER ;