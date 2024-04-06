DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_treconcontext` $$
CREATE PROCEDURE `pr_recon_mst_treconcontext`
(
  in in_recon_code varchar(32),
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32)
)
me:BEGIN

  declare v_master_syscode text default '';

  SET v_master_syscode = (select master_syscode from admin_mst_tusercontext
    where user_code=in_user_code
    and parent_master_syscode ='QCD_L3'
    and delete_flag = 'N');

  set v_master_syscode = ifnull(v_master_syscode,'');

  if v_master_syscode <> '' then
	  INSERT INTO admin_mst_treconcontext
    (
      recon_code,
      parent_master_syscode,
      master_syscode,
      active_status,
      insert_date,
      insert_by
    )
    value
    (
      in_recon_code,
      'QCD_L3',
      v_master_syscode,
      'Y',
      sysdate(),
      in_user_code
    );
  end if;
END $$

DELIMITER ;