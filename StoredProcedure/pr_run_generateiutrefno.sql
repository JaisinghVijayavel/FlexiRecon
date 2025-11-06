DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_generateiutrefno` $$
CREATE PROCEDURE `pr_run_generateiutrefno`
(
  in in_recon_code text,
  out out_msg text,
  out out_result int
)
me:BEGIN
  /*
    Created By : Vijayavel
    Created Date : 29-10-2025

    Updated By :
    updated Date :

    Version : 1
  */

  declare v_sql text default '';
  declare v_tran_table text default '';
  declare v_tranbrkp_table text default '';
  declare v_dataset_db_name text default '';

  declare v_concurrent_ko_flag text default '';

  -- concurrent KO flag
  set v_concurrent_ko_flag = fn_get_configvalue('concurrent_ko_flag');

  if v_concurrent_ko_flag = 'Y' then
	  set v_tran_table = concat(in_recon_code,'_tran');
	  set v_tranbrkp_table = concat(in_recon_code,'_tranbrkp');
  else
	  set v_tran_table = 'recon_trn_ttran';
	  set v_tranbrkp_table = 'recon_trn_ttranbrkp';
  end if;

  -- get dataset db name
  set v_dataset_db_name = fn_get_configvalue('dataset_db_name');

  set v_dataset_db_name = ifnull(v_dataset_db_name,'');

  if v_dataset_db_name <> '' then
    set v_dataset_db_name = concat(v_dataset_db_name,'.');
  end if;

  set v_sql = concat("
	update ",v_tran_table," as a set
		a.col56 = 'Y',
		a.col57 = null,
		a.col58 = null,
		a.col60 = null
	where a.recon_code = '",in_recon_code,"'
	and a.delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg2,@result2);

  set v_sql = concat("
	update ",v_tran_table," as a
	inner join ",v_dataset_db_name,"DS359 as b on a.col13 like b.col3 set
		a.col60 = concat('TH',b.col1,'_',a.col43,'_',date_format(sysdate(),'%d%m%y'))
	where a.recon_code = '",in_recon_code,"'
	and a.col13 <> ''
	and a.delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg2,@result2);

  set v_sql = concat("
	update ",v_tran_table," as a
	inner join ",v_dataset_db_name,"DS359 as b on a.col13 like b.col3 set
		a.col58 = concat('PD',b.col1,'_',a.col43,'_',date_format(sysdate(),'%d%m%y'))
	where a.recon_code = '",in_recon_code,"'
	and a.col13 <> ''
	and a.delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg2,@result2);

  set v_sql = concat("
	update ",v_tran_table," as a
	inner join ",v_dataset_db_name,"DS359 as b on a.col13 like b.col3 set
		a.col57 = concat(b.col1,'-',b.col4)
	where a.recon_code = '",in_recon_code,"'
	and a.col13 <> ''
	and a.delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg2,@result2);

  set v_sql = concat("
	update ",v_tran_table," as a
		set a.col56 = null
	where a.recon_code = '",in_recon_code,"'
	and a.col57 is null
	and a.delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg2,@result2);

  set out_result = 1;
  set out_msg = 'Success';
end $$

DELIMITER ;