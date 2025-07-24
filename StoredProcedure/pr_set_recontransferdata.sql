DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_recontransferdata` $$
CREATE PROCEDURE `pr_set_recontransferdata`
(
  in in_recon_code varchar(32),
  out out_msg text,
  out out_result int
)
begin

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';

	declare v_tranko_table text default '';
	declare v_tranbrkpko_table text default '';

	declare v_ko_table text default '';
	declare v_kodtl_table text default '';
	declare v_koroundoff_table text default '';

  declare v_sql text default '';
  declare v_table text default '';
  declare v_table_prefix text default '';

  declare v_condition text default '';

  -- recon tran table
	set v_tran_table = concat(in_recon_code,'_tran');
	set v_tranbrkp_table = concat(in_recon_code,'_tranbrkp');

	set v_tranko_table = concat(in_recon_code,'_tranko');
	set v_tranbrkpko_table = concat(in_recon_code,'_tranbrkpko');

	set v_ko_table = concat(in_recon_code,'_ko');
	set v_kodtl_table = concat(in_recon_code,'_kodtl');
	set v_koroundoff_table = concat(in_recon_code,'_koroundoff');

  -- transfer tran table
  set v_sql = concat("
    insert into ",v_tran_table,"
    select * from recon_trn_ttran1
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N'
    ");

  call pr_run_sql2(v_sql,@msg2,@result2);

  -- transfer tranko table
  set v_sql = concat("
    insert into ",v_tranko_table,"
    select * from recon_trn_ttranko1
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N'
    ");

  call pr_run_sql2(v_sql,@msg2,@result2);

  -- transfer tranbrkp table
  set v_sql = concat("
    insert into ",v_tranbrkp_table,"
    select * from recon_trn_ttranbrkp1
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N'
    ");

  call pr_run_sql2(v_sql,@msg2,@result2);

  -- transfer tranko table
  set v_sql = concat("
    insert into ",v_tranbrkpko_table,"
    select * from recon_trn_ttranbrkpko1
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N'
    ");

  call pr_run_sql2(v_sql,@msg2,@result2);

  -- transfer ko table
  set v_sql = concat("
    insert into ",v_ko_table,"
    select * from recon_trn_tko1
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N'
    ");

  call pr_run_sql2(v_sql,@msg2,@result2);

  -- transfer kodtl table
  set v_sql = concat("
    insert into ",v_kodtl_table,"
    select a.* from recon_trn_tkodtl1 as a
    inner join recon_trn_tko1 as b on a.ko_gid = b.ko_gid
      and b.delete_flag = 'N'
    where b.recon_code = '",in_recon_code,"'
    and a.delete_flag = 'N'
    ");

  call pr_run_sql2(v_sql,@msg2,@result2);

  -- transfer koroundoff table
  set v_sql = concat("
    insert into ",v_koroundoff_table,"
    select a.* from recon_trn_tkoroundoff1 as a
    inner join recon_trn_tko1 as b on a.ko_gid = b.ko_gid
      and b.delete_flag = 'N'
    where b.recon_code = '",in_recon_code,"'
    and a.delete_flag = 'N'
    ");

  call pr_run_sql2(v_sql,@msg2,@result2);

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;