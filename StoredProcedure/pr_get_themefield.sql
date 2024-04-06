DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_themefield` $$
CREATE PROCEDURE `pr_get_themefield`(in in_theme_code varchar(32))
BEGIN
	select
		themefilter_gid,
		a.theme_code,
		recon_field as filter_field_code,
		ifnull(c.recon_field_desc,d.field_alias_name) as filter_field,
		filter_criteria as filter_criteria,
		theme_seqno,
		filter_value as ident_value,
		open_parentheses_flag,
		close_parentheses_flag,
		join_condition
	from recon_mst_tthemefilter a
	inner join recon_mst_ttheme b on a.theme_code=b.theme_code
		and b.delete_flag = 'N'
	left join recon_mst_treconfield c on a.recon_field=c.recon_field_name
		and b.recon_code=c.recon_code
		and c.delete_flag = 'N'
	left join recon_mst_tfieldstru d on a.recon_field=d.field_name
		and d.delete_flag = 'N'
  where a.theme_code = in_theme_code
  and a.active_status='Y'
  and a.delete_flag = 'N';
END $$

DELIMITER ;