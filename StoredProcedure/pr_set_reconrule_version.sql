DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_reconrule_version` $$
CREATE PROCEDURE `pr_set_reconrule_version`
(
  in in_recon_code text,
  in in_rule_code text,
  in in_reconrule_version text,
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_err_flag boolean default false;
  declare v_err_msg text default '';

  declare v_txt text default '';
  declare v_rule_code text default '';
  declare v_system_match_flag varchar(32) default '';
  declare v_probable_match_flag varchar(32) default '';
  declare v_hold_flag varchar(32) default '';

  declare i int default 0;

  drop temporary table if exists recon_tmp_trule;

  CREATE temporary TABLE recon_tmp_trule(
    rule_code varchar(32) not null,
    system_match_flag varchar(32),
    probable_match_flag varchar(32),
    hold_flag varchar(32),
    PRIMARY KEY (rule_code)
  ) ENGINE = MyISAM;

  -- validate recon
  if not exists(select recon_code from recon_mst_trecon
    where recon_code = in_recon_code
    and period_from <= curdate()
    and (period_to >= curdate()
    or until_active_flag = 'Y')
    and active_status = 'Y'
    and delete_flag = 'N') then
    set v_err_msg = concat(v_err_msg,'Invalid recon,');
    set v_err_flag = true;
  end if;

  -- validation recon rule version
  if exists(select recon_code from recon_mst_trulehistory
    where recon_code = in_recon_code
    and recon_rule_version = in_reconrule_version
    and delete_flag = 'N') then
    set v_err_msg = concat(v_err_msg,'Recon rule version already available,');
    set v_err_flag = true;
  end if;

  set v_txt = in_rule_code;

  set i = instr(v_txt,'$');

  if i = 0 then set i = length(v_txt) + 1; end if;

  while (i > 0) do
		set v_rule_code = substr(v_txt,1,i - 1);

		set v_system_match_flag = ifnull(SPLIT(v_rule_code,'#',2),'');
		set v_hold_flag = ifnull(SPLIT(v_rule_code,'#',3),'');
    set v_probable_match_flag= ifnull(SPLIT(v_rule_code,'#',4),'');
		set v_rule_code = ifnull(SPLIT(v_rule_code,'#',1),'');

		if not exists(select rule_code from recon_mst_trule
			where rule_code = v_rule_code
			and active_status = 'Y'
			and delete_flag = 'N') then
			set v_err_msg = concat(v_err_msg,'Invalid rule code - ',v_rule_code,',');
			set v_err_flag = true;
		end if;

		if v_system_match_flag <> 'Y' and v_system_match_flag <> 'N' then
			set v_err_msg = concat(v_err_msg,'Invalid system match flag - ',v_rule_code,',');
			set v_err_flag = true;
		end if;

		if v_hold_flag <> 'Y' and v_hold_flag <> 'N' then
			set v_err_msg = concat(v_err_msg,'Invalid hold flag - ',v_rule_code,',');
			set v_err_flag = true;
		end if;

		-- insert into temporary rule table
		insert into recon_tmp_trule select v_rule_code,v_system_match_flag,v_probable_match_flag,v_hold_flag;

		set v_txt = substr(v_txt,i+1);
    set i = instr(v_txt,'$');

    if i = 0 and v_txt <> '' then set i = length(v_txt) + 1; end if;
  end while;

  if v_err_flag = true then
    set out_msg = v_err_msg;
    set out_result = 0;
    leave me;
  end if;

  -- update the reconrule_version
  update recon_mst_trecon set
    recon_rule_version = in_reconrule_version,
    update_by = in_user_code,
    update_date = sysdate()
  where recon_code = in_recon_code
  and delete_flag = 'N';

  -- Rule bulk update
  update recon_mst_trule as a
  inner join recon_tmp_trule as b on a.rule_code = b.rule_code
  set
    a.system_match_flag = b.system_match_flag,
    a.hold_flag = b.hold_flag,
    a.probable_match_flag=b.probable_match_flag,
    a.recon_rule_version = in_reconrule_version
  where a.recon_code = in_recon_code
  and a.delete_flag = 'N';

  replace into recon_mst_trulehistory
  (
    rule_code, 
		rule_name, 
		recon_code, 
		rule_apply_on, 
		rule_order,
    source_dataset_code, 
		source_acc_mode, 
		comparison_dataset_code, 
		comparison_acc_mode,
    group_flag, 
		group_method_flag, 
		manytomany_match_flag,
    reversal_flag,
		system_match_flag, 
		manual_match_flag, 
		hold_flag,
    period_from,
		period_to,
		until_active_flag,
		probable_match_flag,
    recon_rule_version,
    active_status,
    insert_date,
    insert_by
  )
  select
    rule_code,
		rule_name,
		recon_code,
		rule_apply_on,
		rule_order,
    source_dataset_code, 
		source_acc_mode, 
		comparison_dataset_code, 
		comparison_acc_mode,
    group_flag, 
		group_method_flag, 
		manytomany_match_flag,
    reversal_flag, 
		system_match_flag, 
		manual_match_flag, 
		hold_flag,
    period_from, 
		period_to, 
		until_active_flag,
		probable_match_flag,
    recon_rule_version,
    active_status,
    sysdate(),
    in_user_code
  from recon_mst_trule
  where recon_code = in_recon_code
  and recon_rule_version = in_reconrule_version
  and delete_flag = 'N';

  drop temporary table if exists recon_tmp_trule;

  set out_msg = 'Recon rule version updated successfully !';
  set out_result = 1;
END $$

DELIMITER ;