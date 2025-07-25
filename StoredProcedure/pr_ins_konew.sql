DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_ins_ko` $$
CREATE PROCEDURE `pr_ins_ko`
(
  in in_recon_code varchar(32),
  in in_job_gid int,
  in in_rule_code varchar(32),
  in in_manual_matchoff char(1),
  in in_reversal_flag char(1),
  in in_ko_value double(15,2),
  in in_ko_reason varchar(255),
  in in_ko_remark varchar(255),
  in in_action_by varchar(32),
  out out_ko_gid int,
  out out_msg text,
  out out_result int(10)
)
me:BEGIN
  /*
    Created By : Vijayavel
    Created Date :

    Updated By : Vijayavel
    updated Date : 25-07-2025

    Version : 1
  */

  declare v_concurrent_ko_flag text default '';
	declare v_ko_table text default '';

  declare v_sql text default '';
  declare v_ko_gid int default 0;
  declare err_msg text default '';
  declare err_flag boolean default false;

  /*
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    set out_msg = 'SQLEXCEPTION';
    set out_result = 0;
  END;
  */

  -- concurrent KO flag
  set v_concurrent_ko_flag = fn_get_configvalue('concurrent_ko_flag');

  if v_concurrent_ko_flag = 'Y' then
	  set v_ko_table = concat(in_recon_code,'_ko');
  else
	  set v_ko_table = 'recon_trn_tko';
  end if;

  set in_rule_code = ifnull(in_rule_code,'');
  set in_ko_reason = ifnull(in_ko_reason,'');
  set in_ko_remark = ifnull(in_ko_remark,'');

  if not exists(select recon_code from recon_mst_trecon
    where recon_code = in_recon_code
    and period_from <= curdate()
    and (period_to >= curdate()
    or until_active_flag = 'Y')
    and active_status = 'Y'
    and delete_flag = 'N') then
    set err_msg := concat(err_msg,'Invalid recon,');
    set err_flag := true;
  end if;

  if in_manual_matchoff <> 'Y' then
    if not exists(select rule_code from recon_mst_trule
      where rule_code = in_rule_code
      and recon_code = in_recon_code
      and active_status = 'Y'
      and delete_flag = 'N') then
      set err_msg := concat(err_msg,'Invalid rule,');
      set err_flag := true;
    end if;
  end if;

  if in_manual_matchoff <> 'Y' and in_manual_matchoff <> 'N' then
    set err_msg := concat(err_msg,'Invalid match off flag,');
    set err_flag := true;
  end if;

  if err_flag = true then
    set out_result = 0;
    set out_msg = err_msg;
    leave me;
  end if;

  set v_sql = concat("
  insert into ",v_ko_table,"
  (
    job_gid,
    ko_date,
    ko_value,
    recon_code,
    rule_code,
    manual_matchoff,
    reversal_flag,
    ko_reason,
    ko_remark,
    insert_date,
    insert_by
  )
  values
  (
    ",cast(in_job_gid as nchar),",
    curdate(),
    ",cast(in_ko_value as nchar),",
    '",in_recon_code,"',
    '",in_rule_code,"',
    '",in_manual_matchoff,"',
    '",in_reversal_flag,"',
    '",in_ko_reason,"',
    '",in_ko_remark,"',
    sysdate(),
    '",in_action_by,"'
  )");

  call pr_run_sql1(v_sql,@msg101,@result101);

  set v_sql = concat("select max(ko_gid) into @v_ko_gid from ",v_ko_table);

  call pr_run_sql1(v_sql,@msg101,@result101);

  set out_ko_gid = ifnull(@v_ko_gid,0);

  set out_result = 1;
  set out_msg = 'Record saved successfully !';
 END $$

DELIMITER ;