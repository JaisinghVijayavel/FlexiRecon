DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_kojobrollback`$$
CREATE PROCEDURE `pr_set_kojobrollback`
(
  in in_jobrollback_gid int,
  in in_recon_code varchar(32),
  in in_jobrollback_reason text,
  in in_ip_addr varchar(128),
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN

  /*
    Created By : Muthu
    Created Date : 19-03-2025

    Updated By : Muthu
    updated Date : 19-03-2025

    Version : 1
  */

	declare v_count int default 0;
  declare v_increment int default 1;
	declare v_job_gid int default 0;
  declare v_out_job_gid int default 0;

  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
		GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE,
		@errno = MYSQL_ERRNO, @text = MESSAGE_TEXT;

		SET @full_error = CONCAT("ERROR ", @errno, " (", @sqlstate, "): ",@text,' ',ifnull(@err_msg,''));

		-- ROLLBACK;

		call pr_upd_job(v_out_job_gid,'F',@full_error,@msg,@result);

		set @out_msg = @full_error;
		set @out_result = 0;

		SIGNAL SQLSTATE '99999' SET
		MYSQL_ERRNO = @errno,
		MESSAGE_TEXT = @text;
  END;

  drop temporary table if exists recon_tmp_tjob;
  create temporary table recon_tmp_tjob (id int primary key auto_increment,job_gid int not null);

  if not exists(select 1 from recon_trn_tjob
    where jobtype_code = 'UJ' and delete_flag = 'N'
    and job_status not in('C','F'))then
		call pr_ins_job(in_recon_code,'UJ',0,concat('Undo Job - ',ifnull(in_jobrollback_reason,'')),'',in_user_code,in_ip_addr,'I'
						,'Initiated.',@out_job_gid,@msg,@result);
		set v_out_job_gid = @out_job_gid;
	else
		call pr_ins_job(in_recon_code,'UJ',0,concat('Undo Job - ',ifnull(in_jobrollback_reason,'')),'',in_user_code,in_ip_addr,'F','Undo job already running!'
						,@out_job_gid,@msg,@result);
		set v_out_job_gid = @out_job_gid;
		call pr_upd_job(v_out_job_gid,'F','Undo Job already running!',@msg,@result);
		set out_result = 0;
		set out_msg = 'Undo job already running!';
    leave me;
  end if;

	if exists(select 1 from recon_trn_tjob where recon_code = in_recon_code
				and job_gid = in_jobrollback_gid
        and job_status in ('C','F','R')
        and delete_flag = 'N'
        and jobtype_code in ('A','S','M')) then

    insert into recon_tmp_tjob (job_gid)
		  select job_gid from recon_trn_tjob
		  where recon_code = in_recon_code
      and job_gid >= in_jobrollback_gid
      and jobtype_code in ('A','S','M')
		  and job_status in ('C','F','R')
		  and delete_flag = 'N'
      order by job_gid desc;

    select count(*) into v_count from recon_tmp_tjob;

    while(v_count >= v_increment) do
      select job_gid into v_job_gid from recon_tmp_tjob where id = v_increment;

      call pr_upd_job(v_out_job_gid,'P',concat('Processing job id - ',cast(v_job_gid as nchar(32))),@msg,@result);

			call pr_set_undokojob(in_recon_code,v_job_gid,concat('Undo job id - ',cast(v_job_gid as nchar),' - ',in_jobrollback_reason)
									,in_ip_addr,in_user_code,@out_msg,@out_result);

      set v_increment = v_increment + 1;
    end while;

    if v_count > 0 then
      call pr_upd_job(v_out_job_gid,'C','Undo Job completed.',@msg,@result);

      set out_result = 1;
		  set out_msg = 'Success.';
    else
		  call pr_upd_job(v_out_job_gid,'F','Invalid job!',@msg,@result);

		  set out_result = 0;
		  set out_msg = 'Invalid job!';
    end if;
	else
		call pr_upd_job(v_out_job_gid,'F','Invalid job!',@msg,@result);

		set out_result = 0;
		set out_msg = 'Invalid job!';
  end if;
END $$

DELIMITER ;