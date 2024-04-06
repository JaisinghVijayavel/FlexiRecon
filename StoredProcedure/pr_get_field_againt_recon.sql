DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_field_againt_recon` $$
CREATE PROCEDURE `pr_get_field_againt_recon`
(
  in in_recon_code varchar(32),
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32)
)
BEGIN
  select a.* from
  (
		select
			recon_field_name,
			recon_field_desc,
			ifnull(recon_field_type,'') as recon_field_type,
			display_order
		from recon_mst_treconfield
		where recon_code = in_recon_code
		and active_status = 'Y'
		and delete_flag = 'N'
		union
		select
			a.field_name as recon_field_name,
			b.field_alias_name as recon_field_desc,
			ifnull(b.field_org_type,'') as recon_field_type,
			display_order + 500 as display_order
		from recon_mst_tsystemfield as a
		inner join recon_mst_tfieldstru as b on a.field_name = b.field_name
			and b.delete_flag = 'N'
		where a.table_name = 'recon_trn_ttran'
		and a.active_status = 'Y'
		and a.delete_flag = 'N'
  ) as a
  order by a.display_order;
END $$

DELIMITER ;