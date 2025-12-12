DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_reconfieldqcdlist` $$
CREATE PROCEDURE `pr_get_reconfieldqcdlist`
(
  in in_recon_code varchar(32),
  in in_recon_field_name varchar(32),
  in in_depend_value text,
  in in_tran_gid int,
  in in_tranbrkp_gid int,
  out out_msg text,
  out out_result int
)
BEGIN
  /*
    Created By : Vijayavel
    Created Date: 03-12-2025

    Updated By : Vijayavel
    updated Date : 04-12-2025

	  Version - 2
  */
  declare v_concurrent_ko_flag text default '';

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';

	declare v_qcd_parent_code text default '';
	declare v_qcd_multivalue_flag text default '';
	declare v_depend_field_flag text default '';
	declare v_depend_recon_field text default '';
	declare v_depend_qcd_parent_code text default '';
	declare v_depend_master_code text default '';
	declare v_depend_value text default '';

	declare v_sql text default '';

  set out_result = 0;
  set out_msg = 'Failed';

  -- concurrent KO flag
  set v_concurrent_ko_flag = fn_get_configvalue('concurrent_ko_flag');

  if v_concurrent_ko_flag = 'Y' then
		if in_tranbrkp_gid > 0 then
			set v_tranbrkp_table = concat(in_recon_code,'_tranbrkp');
		else
			set v_tran_table = concat(in_recon_code,'_tran');
		end if;
  else
		if in_tranbrkp_gid > 0 then
			set v_tranbrkp_table = 'recon_trn_ttranbrkp';
		else
			set v_tran_table = 'recon_trn_ttran';
		end if;
  end if;

  -- get recon field
  select
    qcd_parent_code,
    qcd_multivalue_flag,
    depend_field_flag,
    depend_recon_field
  into
    v_qcd_parent_code,
    v_qcd_multivalue_flag,
    v_depend_field_flag,
    v_depend_recon_field
  from recon_mst_treconfield
  where recon_code = in_recon_code
  and recon_field_name = in_recon_field_name
  and active_status = 'Y'
  and delete_flag = 'N';

  set v_qcd_parent_code = ifnull(v_qcd_parent_code,'');
  set v_qcd_multivalue_flag = ifnull(v_qcd_multivalue_flag,'');
  set v_depend_field_flag = ifnull(v_depend_field_flag,'');
  set v_depend_recon_field = ifnull(v_depend_recon_field,'');

  if v_depend_field_flag = 'Y' then
		set v_depend_value = ifnull(in_depend_value,'');

    -- get depend qcd
    select
      qcd_parent_code
    into
      v_depend_qcd_parent_code
    from recon_mst_treconfield
    where recon_code = in_recon_code
    and recon_field_name = v_depend_recon_field
    and active_status = 'Y'
    and delete_flag = 'N';

    set v_depend_qcd_parent_code = ifnull(v_depend_qcd_parent_code,'');

    if v_depend_qcd_parent_code <> '' then
      select
        master_syscode
      into
        v_depend_master_code
      from recon_mst_tmaster
      where parent_master_syscode = v_depend_qcd_parent_code
      and master_name = v_depend_value
      and active_status = 'Y'
      and delete_flag = 'N';

      set v_depend_master_code = ifnull(v_depend_master_code,'');
    end if;
  end if;

  set v_depend_master_code = ifnull(v_depend_master_code,'');

  -- get qcd value
  if v_depend_master_code = '' then
    select * from recon_mst_tmaster
    where parent_master_syscode = v_qcd_parent_code
    and active_status = 'Y'
    and delete_flag = 'N';
  else
    select * from recon_mst_tmaster
    where parent_master_syscode = v_qcd_parent_code
    and depend_parent_master_syscode = v_depend_qcd_parent_code
    and depend_master_syscode = v_depend_master_code
    and active_status = 'Y'
    and delete_flag = 'N';
  end if;

  set out_result = 1;
  set out_msg = 'Success';
END $$

DELIMITER ;