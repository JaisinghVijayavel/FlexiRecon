DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_manualupdate` $$
CREATE PROCEDURE `pr_set_manualupdate`
(
  in in_scheduler_gid text,
  out out_msg text,
  out out_result int
)
me:BEGIN
	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';

  declare v_sql text default '';

	set v_tran_table = 'recon_trn_ttran';
	set v_tranbrkp_table = 'recon_trn_ttranbrkp';

  set v_sql = concat("update recon_trn_tmanualtran as a
    inner join ",v_tran_table," as b on a.tran_gid = b.tran_gid
      and b.delete_flag = 'N'
    set
      a.recon_code = b.recon_code,
      a.dataset_code = b.dataset_code,
      a.ko_mult = b.tran_mult,
      a.ko_acc_mode = b.tran_acc_mode
    where a.scheduler_gid = ",cast(in_scheduler_gid as nchar),"
    and a.tranbrkp_gid = 0
    and a.delete_flag = 'N'");

  call pr_run_sql1(v_sql,@msg,@result);

  set v_sql = concat("update recon_trn_tmanualtran as a
    inner join ",v_tranbrkp_table," as b on a.tran_gid = b.tran_gid
      and a.tranbrkp_gid = b.tranbrkp_gid
      and b.delete_flag = 'N'
    set
      a.recon_code = b.recon_code,
      a.dataset_code = b.tranbrkp_dataset_code,
      a.ko_mult = b.tran_mult,
      a.ko_acc_mode = b.tran_acc_mode
    where a.scheduler_gid = ",cast(in_scheduler_gid as nchar),"
    and a.tranbrkp_gid > 0
    and a.delete_flag = 'N'");

  call pr_run_sql1(v_sql,@msg,@result);
end $$

DELIMITER ;