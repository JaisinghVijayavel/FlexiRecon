DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_tdatasetcontext` $$
CREATE PROCEDURE `pr_recon_mst_tdatasetcontext`
(
  in in_dataset_code varchar(32),
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32)
)
me:BEGIN

  declare v_master_syscode text default '';

  SET v_master_syscode  = (select master_syscode from admin_mst_tusercontext
    where user_code=in_user_code
    and parent_master_syscode ='QCD_L1'
    and delete_flag = 'N');

  set v_master_syscode = ifnull(v_master_syscode,'');

	INSERT INTO admin_mst_tdatasetcontext
  (
    dataset_code,
    parent_master_syscode,
    master_syscode,
    active_status,
    insert_date,
    insert_by
  )
  value
  (
    in_dataset_code,
    'QCD_L1',
    v_master_syscode,
    'A',
    sysdate(),
    in_user_code
  );
END $$

DELIMITER ;