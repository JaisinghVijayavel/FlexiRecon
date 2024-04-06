DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_treporttemplatesorting` $$
CREATE PROCEDURE `pr_recon_mst_treporttemplatesorting`
(
	inout in_reporttemplatesorting_gid int,
	in in_reporttemplate_code varchar(32),
	in in_report_field varchar(32),
	in in_sorting_order decimal(7, 3),
	in in_active_status varchar(32),
	in in_action varchar(32),
	in in_action_by varchar(32),
	in in_delete_flag varchar(32),
	out out_msg text,
	out out_result int
)
me:BEGIN
	/*
		Created By : Hema
		Created Date : Feb-13-2023

		Updated By : Vijayavel
		Updated Date : 29-03-2024

		Version No : 1
	*/

	declare err_msg text default '';
	declare err_flag boolean default false;
	declare v_msg text default '';
	declare v_reporttemplatesorting_gid int default 0;

	if(in_action = 'INSERT' or in_action = 'UPDATE') then
		if in_reporttemplate_code = '' or in_reporttemplate_code is null then
				set err_msg := concat(err_msg,'Report templetecode cannot be empty,');
				set err_flag := true;
		end if;

		if in_sorting_order = '' or in_sorting_order is null then
				set err_msg := concat(err_msg,'Sorting order cannot be empty,');
				set err_flag := true;
		end if;

		if in_report_field = '' or in_report_field is null then
				set err_msg := concat(err_msg,'Report field cannot be empty,');
				set err_flag := true;
		end if;

    if err_flag = true then
		  set out_result = 0;
		  set out_msg = err_msg;
      leave me;
    end if;
	end if;

	if(in_action = 'INSERT') then
		set in_reporttemplatesorting_gid = 0;

		insert into recon_mst_treporttemplatesorting
		(
			reporttemplate_code,
			report_field,
			sorting_order,
			active_status,
			insert_date,
			insert_by
		)
		value
		(
			in_reporttemplate_code,
			in_report_field,
			in_sorting_order,
			in_active_status,
			sysdate(),
			in_action_by
		);

		select
			max(reporttemplatesorting_gid)
		into
			v_reporttemplatesorting_gid
		from recon_mst_treporttemplatesorting;

		set in_reporttemplatesorting_gid = v_reporttemplatesorting_gid;
		set out_result = 1;
		set out_msg = 'Record saved successfully.. !';
	elseif(in_action = 'UPDATE') then
		update recon_mst_treporttemplatesorting set
			report_field = in_report_field,
			sorting_order = in_sorting_order,
			active_status = in_active_status,
			update_date = sysdate(),
			update_by = in_user_code
		where reporttemplatesorting_gid = in_reporttemplatesorting_gid
		and delete_flag = 'N';

		set out_result = 1;
		set out_msg = 'Record Updated Successfully.. !';
	elseif(in_action = 'DELETE') then
		update recon_mst_treporttemplatesorting set
			active_status = 'N',
			update_date = sysdate(),
			update_by = in_user_code,
			delete_flag = 'Y'
		where reporttemplatesorting_gid = in_reporttemplatesorting_gid
		and delete_flag = 'N';

		set out_result = 1;
		set out_msg = 'Record deleted successfully.. !';
	end if;
END $$

DELIMITER ;