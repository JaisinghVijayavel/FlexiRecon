DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_fetch_reconversionhistory` $$
CREATE PROCEDURE `pr_fetch_reconversionhistory`
(
  in in_recon_code varchar(32),
  in in_version_code varchar(32),
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32)
)
BEGIN
	select 
		rulehistory_gid,
		a.rule_code,
		rule_name,
		source_dataset_code,
		recon_rule_version,
		c.dataset_name as source_dataset_desc,
		comparison_dataset_code,
		d.dataset_name as comparison_dataset_desc,
		a.recon_rule_version,
		system_match_flag,
		case system_match_flag 
			when 'Y' then 'YES' 
			else 'NO' 
		end as system_match_flag_desc,
		hold_flag,
		case hold_flag 
			when 'Y' then 'YES' 
			else 'NO' 
		end as hold_flag_desc ,
		case probable_match_flag 
			when 'Y' then 'YES' 
			else 'NO' 
		end as probable_match_desc 
	from recon_mst_trulehistory a
	inner join recon_mst_tdataset c on a.source_dataset_code=c.dataset_code
		and c.delete_flag = 'N' 
	inner join recon_mst_tdataset d on a.source_dataset_code=d.dataset_code
		and d.delete_flag = 'N' 
	where a.recon_code=in_recon_code 
	and a.recon_rule_version=in_version_code
	and a.delete_flag = 'N';
END $$

DELIMITER ;