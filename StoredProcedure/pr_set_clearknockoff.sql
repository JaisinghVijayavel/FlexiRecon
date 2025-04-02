DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_clearknockoff` $$
CREATE PROCEDURE `pr_set_clearknockoff`
(
  in in_recon_code varchar(32)
)
begin
  declare v_sql text default '';

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';

	declare v_tranko_table text default '';
	declare v_tranbrkpko_table text default '';

	declare v_ko_table text default '';
	declare v_kodtl_table text default '';
	declare v_koroundoff_table text default '';

  declare v_concurrent_ko_flag text default '';

  -- concurrent KO flag
  set v_concurrent_ko_flag = fn_get_configvalue('concurrent_ko_flag');

  if v_concurrent_ko_flag = 'Y' then
	  set v_tran_table = concat(in_recon_code,'_tran');
	  set v_tranbrkp_table = concat(in_recon_code,'_tranbrkp');

	  set v_tranko_table = concat(in_recon_code,'_tranko');
	  set v_tranbrkpko_table = concat(in_recon_code,'_tranbrkpko');

	  set v_ko_table = concat(in_recon_code,'_ko');
	  set v_kodtl_table = concat(in_recon_code,'_kodtl');
	  set v_koroundoff_table = concat(in_recon_code,'_koroundoff');
  else
	  set v_tran_table = 'recon_trn_ttran';
	  set v_tranbrkp_table = 'recon_trn_ttranbrkp';

	  set v_tranko_table = 'recon_trn_ttranko';
	  set v_tranbrkpko_table = 'recon_trn_ttranbrkpko';

	  set v_ko_table = 'recon_trn_tko';
	  set v_kodtl_table = 'recon_trn_tkodtl';
	  set v_koroundoff_table = 'recon_trn_tkoroundoff';
  end if;

  -- kodtl
  set v_sql = concat("
  delete from ",v_kodtl_table,"
  where ",v_kodtl_table,".ko_gid in
  (
    select b.ko_gid from ",v_ko_table," as b
    where b.recon_code = '",in_recon_code,"'
    and b.ko_gid = ",v_kodtl_table,".ko_gid
    and b.delete_flag = 'N'
  )
  and ",v_kodtl_table,".delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg2,@result2);

  -- koroundoff
  set v_sql = concat("
  delete from ",v_koroundoff_table,"
  where ",v_koroundoff_table,".ko_gid in
  (
    select b.ko_gid from ",v_ko_table," as b
    where b.recon_code = '",in_recon_code,"'
    and b.ko_gid = ",v_koroundoff_table,".ko_gid
    and b.delete_flag = 'N'
  )
  and ",v_koroundoff_table,".delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg2,@result2);

  -- ko
  set v_sql = concat("
  delete from ",v_ko_table,"
  where recon_code = '",in_recon_code,"'
  and delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg2,@result2);

  -- tran
  set v_sql = concat("
  insert into ",v_tran_table,"
    select * from ",v_tranko_table,"
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg2,@result2);

  set v_sql = concat("
  update ",v_tran_table," set
    excp_value = tran_value,
    roundoff_value = 0,
    mapped_value = 0,
    theme_code = '',
    ko_gid = 0,
    ko_date = null
  where recon_code = '",in_recon_code,"'
  and delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg2,@result2);

  set v_sql = concat("
  delete from ",v_tranko_table,"
  where recon_code = '",in_recon_code,"'
  and delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg2,@result2);

  -- tranbrkp
  set v_sql = concat("
  insert into ",v_tranbrkp_table,"
    select * from ",v_tranbrkpko_table,"
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg2,@result2);

  set v_sql = concat("
  update ",v_tranbrkp_table," set
    excp_value = tran_value,
    roundoff_value = 0,
    mapped_value = 0,
    theme_code = '',
    tran_gid = 0,
    posted_job_gid = 0,
    posted_rule_code = '',
    ko_gid = 0,
    ko_date = null
  where recon_code = '",in_recon_code,"'
  and delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg2,@result2);

  set v_sql = concat("
  delete from ",v_tranbrkpko_table,"
  where recon_code = '",in_recon_code,"'
  and delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg2,@result2);

  set v_sql = concat("
  update ",v_tran_table," set
    tran_remark2 = ''
  where recon_code = '",in_recon_code,"'
  and tran_remark2 <> ''
  and delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg2,@result2);

  set v_sql = concat("
  update ",v_tranbrkp_table," set
    tran_remark2 = ''
  where recon_code = '",in_recon_code,"'
  and tran_remark2 <> ''
  and delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg2,@result2);
end $$

DELIMITER ;