DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_OFrecon_dupvendor` $$
CREATE PROCEDURE `pr_set_OFrecon_dupvendor`()
begin
	drop temporary table if exists recon_tmp_ttran;

	create temporary table recon_tmp_ttran
	select col1
	from recon_trn_ttran
	where recon_code = 'RE135'
	and delete_flag = 'N'
	group by col1
	having count(*) > 1;

	update recon_trn_ttran set theme_code = 'Duplicate vendor code'
	where recon_code = 'RE135'
	and col1 in (select col1
	from recon_tmp_ttran)
	and delete_flag = 'N';

	drop temporary table recon_tmp_ttran;
end $$

DELIMITER ;