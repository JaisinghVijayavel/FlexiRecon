DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_recondataset` $$
CREATE PROCEDURE `pr_get_recondataset`
(
  in in_recon_code varchar(32)
)
me:BEGIN
  select
    a.dataset_code,
    b.dataset_name
  from recon_mst_trecondataset a
  inner join recon_mst_tdataset b on a.dataset_code = b.dataset_code
    and b.delete_flag = 'N'
  where a.recon_code = in_recon_code
  and a.active_status = 'Y'
  and a.delete_flag = 'N';
END $$

DELIMITER ;