DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_tranbrkpreport` $$
CREATE PROCEDURE `pr_run_tranbrkpreport`
(
  in in_job_gid int,
  in in_rptsession_gid int,
  in in_condition text,
  in in_user_code varchar(16),
  out out_msg text,
  out out_result int
)
me:BEGIN
  /*
    Created By : Vijayavel
    Created Date : 28-07-2023

    Updated By : Vijayavel
    updated Date :

    Version : 1
  */

  declare v_sql text default '';

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  /*
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE,
    @errno = MYSQL_ERRNO, @text = MESSAGE_TEXT;

    set @text = concat(@text,' ',err_msg);

    SET @full_error = CONCAT("ERROR ", @errno, " (", @sqlstate, "): ", @text);

    ROLLBACK;

    if in_outputfile_flag then
      call pr_upd_job(v_job_gid,'F',@full_error,@msg,@result);
    end if;

    set out_msg = @full_error;
    set out_result = 0;
  END;
  */

  set in_job_gid = ifnull(in_job_gid,0);
  set in_rptsession_gid = ifnull(in_rptsession_gid,0);
  set in_user_code = ifnull(in_user_code,'');

  set v_sql = concat(v_sql,"insert into recon_rpt_ttranbrkp
		select
		  ",cast(in_rptsession_gid as nchar)," as rptsession_gid,
		  ",cast(in_job_gid as nchar)," as job_gid,
      '", in_user_code ,"' as user_code,
      b.dataset_name as tranbrkp_name,
      ifnull(c.tran_value,d.tran_value) as base_value,
      ifnull(c.tran_acc_mode,d.tran_acc_mode) as base_acc_mode,
      a.*
		from recon_trn_ttranbrkp as a
    left join recon_mst_tdataset as b on a.tranbrkp_dataset_code = b.dataset_code
    and b.delete_flag = 'N'
    left join recon_trn_ttran as c on a.tran_gid = c.tran_gid and c.delete_flag = 'N'
    left join recon_trn_ttranko as d on a.tran_gid = d.tran_gid and d.delete_flag = 'N'
		where true ", in_condition," and a.delete_flag = 'N'

    union

		select
		  ",cast(in_rptsession_gid as nchar)," as rptsession_gid,
		  ",cast(in_job_gid as nchar)," as job_gid,
      '", in_user_code ,"' as user_code,
      b.dataset_name as tranbrkp_name,
      ifnull(c.tran_value,d.tran_value) as base_value,
      ifnull(c.tran_acc_mode,d.tran_acc_mode) as base_acc_mode,
      a.*
		from recon_trn_ttranbrkpko as a
    left join recon_mst_tdataset as b on a.tranbrkp_dataset_code = b.dataset_code
    and b.delete_flag = 'N'
    left join recon_trn_ttran as c on a.tran_gid = c.tran_gid and c.delete_flag = 'N'
    left join recon_trn_ttranko as d on a.tran_gid = d.tran_gid and d.delete_flag = 'N'
		where true ", in_condition," and a.delete_flag = 'N'
  ");

  -- select v_sql;

  call pr_run_sql(v_sql,@msg,@result);

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;