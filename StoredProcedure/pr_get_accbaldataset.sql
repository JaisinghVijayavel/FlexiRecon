DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_accbaldataset` $$
CREATE PROCEDURE `pr_get_accbaldataset`(
	in in_dataset_code varchar(32),
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32)
)
BEGIN
  /*
    Created By :
    Created Date :

    Updated By : Vijayavel
    updated Date : 13-03-2026

    Version : 1
  */

	select
    a.accbal_gid,
    a.dataset_code,
    b.dataset_name,
    a.tran_date,
    date_format(a.tran_date,'%d/%m/%Y') as tran_date_fetch,
    a.bal_value
  from recon_trn_taccbal a
  inner join recon_mst_tdataset b on a.dataset_code = b.dataset_code
  where a.dataset_code = in_dataset_code
  and a.tran_date <> '0000-00-00'
  and a.delete_flag = 'N'
  order by a.tran_date desc;
END $$

DELIMITER ;