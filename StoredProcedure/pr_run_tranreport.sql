DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_tranreport` $$
CREATE PROCEDURE `pr_run_tranreport`(
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

  set @rec_slno := 0;

  set v_sql = concat(v_sql,"insert into recon_rpt_ttran
		select
      ",cast(in_rptsession_gid as nchar),",
		  ",cast(in_job_gid as nchar)," as job_gid,
		  @rec_slno:=@rec_slno+1,
      '", in_user_code ,"',
      b.dataset_name,
      a.*
		from recon_trn_ttran as a
    left join recon_mst_tdataset as b on a.dataset_code = b.dataset_code and b.delete_flag = 'N'
		where a.delete_flag = 'N' ", in_condition,"

    union all

		select
      ",cast(in_rptsession_gid as nchar),",
		  ",cast(in_job_gid as nchar)," as job_gid,
		  @rec_slno:=@rec_slno+1,
      '", in_user_code ,"',
      b.dataset_name,
      a.*
		from recon_trn_ttranko as a
    left join recon_mst_tdataset as b on a.dataset_code = b.dataset_code and b.delete_flag = 'N'
		where a.delete_flag = 'N' ", in_condition,"
  ");

  call pr_run_sql(v_sql,@msg,@result);

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;