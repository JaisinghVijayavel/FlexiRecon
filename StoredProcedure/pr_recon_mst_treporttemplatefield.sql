DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_treporttemplatefield` $$
CREATE PROCEDURE `pr_recon_mst_treporttemplatefield`
(
	in in_reporttemplatefield_gid int,
	in in_reporttemplate_code varchar(32),
	in in_report_field varchar(32),
	in in_display_flag varchar(32),
	in in_display_order decimal(7,3),
	in in_active_status varchar(32),
	in in_action_by varchar(32),
	out out_msg text,
	out out_result int
)
me:BEGIN
	/*
		Created By : Hema
    Created Date : Mar-27-2024

    Updated By : Vijayavel
    Updated Date : 29-03-2024

    Version No : 2
  */
	
	declare v_msg text default '';
	declare err_msg text default '';
	declare err_flag boolean default false;
	
	set in_reporttemplatefield_gid = 0;
	
	insert into recon_mst_treporttemplatefield
	(
		reporttemplatefield_gid,
		reporttemplate_code,
		report_field,
		display_flag,
		display_order,
		active_status,
		insert_date,
		insert_by
	)
	value
	(
		in_reporttemplatefield_gid,
		in_reporttemplate_code,
		in_report_field,
		in_display_flag,
		in_display_order,
		in_active_status, 
		sysdate(),
		in_action_by
	);
	
	set out_result = 1;
	set out_msg = 'Record saved successfully.. !';
 END $$

DELIMITER ;