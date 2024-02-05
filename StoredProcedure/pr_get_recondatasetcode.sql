DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_recondatasetcode` $$
CREATE PROCEDURE `pr_get_recondatasetcode`
(
  in in_recon_code varchar(32),
  in in_dataset_code varchar(32),
  out out_tran_dataset_code varchar(32),
  out out_tranbrkp_dataset_code varchar(32)
)
me:BEGIN
  declare v_dataset_code text default '';
  declare v_parent_dataset_code text default '';

  select
    a.dataset_code,
    b.parent_dataset_code
  into
    v_dataset_code,
    v_parent_dataset_code
  from recon_mst_trecondataset a
  inner join recon_mst_tdataset b on a.dataset_code = b.dataset_code
    and b.delete_flag = 'N'
  where a.recon_code = in_recon_code
  and a.recon_code = in_dataset_code
  and a.active_status = 'Y'
  and a.delete_flag = 'N';

  set v_dataset_code = ifnull(v_dataset_code,'');
  set v_parent_dataset_code = ifnull(v_parent_dataset_code,'');

  if v_parent_dataset_code = '' then
    set out_tran_dataset_code = v_dataset_code;
    set out_tranbrkp_dataset_code = '';
  else
    set out_tran_dataset_code = v_parent_dataset_code;
    set out_tranbrkp_dataset_code = v_dataset_code;
  end if;
END $$

DELIMITER ;