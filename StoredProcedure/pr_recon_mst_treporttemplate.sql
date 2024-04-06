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
    Updated Date : 29-03-2024

    Version No : 1
  */
	declare err_msg text default '';
	declare err_flag boolean default false;
	declare v_msg text default '';
	declare v_reporttemplate_gid int default 0;

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