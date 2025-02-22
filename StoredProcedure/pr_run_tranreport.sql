DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_tranreport` $$
CREATE PROCEDURE `pr_run_tranreport`(
  in in_recon_code varchar(32),
  in in_job_gid int,
  in in_rptsession_gid int,
  in in_condition text,
  in in_sorting_order text,
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
  /*
    Created By : Vijayavel
    Created Date : 28-07-2023

    Updated By : Vijayavel
    updated Date : 22-05-2025

    Version : 5
  */

  declare v_count int default 0;
  declare v_sql text default '';

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  drop temporary table if exists recon_tmp_ttran;

  CREATE temporary TABLE recon_tmp_ttran select * from recon_trn_ttran where 1 = 2;

  alter table recon_tmp_ttran ENGINE = MyISAM;
  alter table recon_tmp_ttran add primary key(tran_gid);

  create index idx_excp_value on recon_tmp_ttran(excp_value);
  create index idx_tran_date on recon_tmp_ttran(tran_date);
  create index idx_recon_code on recon_tmp_ttran(recon_code);
  create index idx_dataset_code on recon_tmp_ttran(recon_code,dataset_code);

  -- delete record
  if in_job_gid = 0 and in_rptsession_gid = 0 then
    delete from recon_rpt_ttran
    where user_code = in_user_code
    and job_gid = 0
    and rptsession_gid = 0;
  end if;

  -- transfer to temporary table
  set v_sql = concat(v_sql,"insert into recon_tmp_ttran
    select z.* from (
		select
      a.*
		from recon_trn_ttran as a
    left join recon_mst_tdataset as b on a.dataset_code = b.dataset_code and b.delete_flag = 'N'
		where a.delete_flag = 'N' ", in_condition,"

    union all

		select
      a.*
		from recon_trn_ttranko as a
    left join recon_mst_tdataset as b on a.dataset_code = b.dataset_code and b.delete_flag = 'N'
		where a.delete_flag = 'N' ", in_condition,"
    LOCK IN SHARE MODE) as z
  ");

  call pr_run_sql(v_sql,@msg,@result);

  -- calc exception value based on roundoff value
  update recon_tmp_ttran set
    excp_value = excp_value - roundoff_value;

  -- transfer records to report table
  set @rec_slno := 0;

  set v_sql = concat("insert into recon_rpt_ttran
		select
      ",cast(in_rptsession_gid as nchar),",
		  ",cast(in_job_gid as nchar)," as job_gid,
		  @rec_slno:=@rec_slno+1,
      '", in_user_code ,"',
      b.dataset_name,
      null as match_gid,
      a.*
		from recon_tmp_ttran as a
    left join recon_mst_tdataset as b on a.dataset_code = b.dataset_code and b.delete_flag = 'N' ",in_sorting_order);

  call pr_run_sql(v_sql,@msg,@result);

  drop temporary table if exists recon_tmp_ttran;

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;