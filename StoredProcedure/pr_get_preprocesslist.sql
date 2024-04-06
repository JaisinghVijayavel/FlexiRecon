DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_preprocesslist` $$
CREATE PROCEDURE `pr_get_preprocesslist`
(
	in in_recon_code  varchar(32),
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32)
)
BEGIN
	select 
		preprocess_gid,
		preprocess_code,
		preprocess_desc,
		a.recon_code,
		b.recon_name,
		process_method,
		fn_get_mastername(a.process_method, 'QCD_PROCESSM') as process_method_desc,
		a.active_status, 
		fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc,
		preprocess_order,
		process_query,
		process_function,
		get_recon_field,
		set_recon_field,
		a.hold_flag, 
		fn_get_mastername(a.hold_flag, 'QCD_YN') as hold_flag_desc
	from recon_mst_tpreprocess  a 
	inner join recon_mst_trecon b on a.recon_code=b.recon_code 
		and b.delete_flag = 'N' 
	where a.recon_code=in_recon_code
	and a.delete_flag = 'N'
  order by preprocess_order;
END $$

DELIMITER ;