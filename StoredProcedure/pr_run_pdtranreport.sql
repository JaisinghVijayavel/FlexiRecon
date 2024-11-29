DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_pdtranreport` $$
CREATE PROCEDURE `pr_run_pdtranreport`(
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
    Created Date : 28-11-2024

    Updated By : Vijayavel
    updated Date :

    Version : 1
  */

  declare v_count int default 0;
  declare v_sql text default '';

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  drop temporary table if exists recon_tmp_tpdtran;

  CREATE temporary TABLE recon_tmp_tpdtran select * from recon_trn_ttran where 1 = 2;

  alter table recon_tmp_tpdtran ENGINE = MyISAM;
  alter table recon_tmp_tpdtran add primary key(tran_gid);

  create index idx_excp_value on recon_tmp_tpdtran(excp_value);
  create index idx_tran_date on recon_tmp_tpdtran(tran_date);
  create index idx_recon_code on recon_tmp_tpdtran(recon_code);
  create index idx_dataset_code on recon_tmp_tpdtran(recon_code,dataset_code);

  -- delete record
  if in_job_gid = 0 and in_rptsession_gid = 0 then
    delete from recon_rpt_ttran
    where user_code = in_user_code
    and job_gid = 0
    and rptsession_gid = 0;
  end if;

  -- transfer to temporary table
  set v_sql = concat(v_sql,"insert into recon_tmp_tpdtran
		select
      a.*
		from recon_trn_ttran as a
    inner join recon_mst_tpdrecon as p on a.recon_code = p.pdrecon_code
      and p.active_status = 'Y'
      and p.delete_flag = 'N'
    left join recon_mst_tdataset as b on a.dataset_code = b.dataset_code and b.delete_flag = 'N'
		where a.delete_flag = 'N' ", in_condition,"

    union all

		select
      a.*
		from recon_trn_ttranko as a
    inner join recon_mst_tpdrecon as p on a.recon_code = p.pdrecon_code
      and p.active_status = 'Y'
      and p.delete_flag = 'N'
    left join recon_mst_tdataset as b on a.dataset_code = b.dataset_code and b.delete_flag = 'N'
		where a.delete_flag = 'N' ", in_condition,"
  ");

  call pr_run_sql(v_sql,@msg,@result);

  -- calc exception value based on roundoff value
  update recon_tmp_tpdtran set
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
		from recon_tmp_tpdtran as a
    left join recon_mst_tdataset as b on a.dataset_code = b.dataset_code and b.delete_flag = 'N' ",in_sorting_order);

  call pr_run_sql(v_sql,@msg,@result);

  drop temporary table if exists recon_tmp_tpdtran;

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;