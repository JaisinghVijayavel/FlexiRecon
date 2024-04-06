DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_upload_templetefile` $$
CREATE PROCEDURE `pr_upload_templetefile`
(
	in in_reporttemplate_code varchar(32),
	in in_file_name varchar(64),
	in in_file_path varchar(64),
	in in_action_by varchar(32),
	out out_msg text,
	out out_result int
)
me:BEGIN
	update recon_mst_treporttemplate set 
		file_name = in_file_name,
		file_path = in_file_path
	where reporttemplate_code = in_reporttemplate_code
	and delete_flag = 'N';
	
	set out_result = 1;
	set out_msg = 'File Uploaded Successfully.. !';
END $$

DELIMITER ;