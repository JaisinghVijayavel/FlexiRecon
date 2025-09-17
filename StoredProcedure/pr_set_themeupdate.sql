DELIMITER $$
DROP PROCEDURE IF EXISTS `pr_set_themeupdate` $$
CREATE procedure `pr_set_themeupdate`
(
  in in_scheduler_gid int,
  in in_job_gid int,
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32),
  out out_msg text,
  out out_result int
)
me:begin
  /*
    Created By : Vijayavel
    Created Date : 26-03-2024

    Updated By : Vijayavel
    Updated Date : 11-09-2025

    Version : 7
  */

  declare v_recon_code text default '';
  declare v_recon_theme_flag text default '';

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';

  declare v_concurrent_ko_flag text default '';

  declare v_sql text default '';
  declare v_txt text default '';

	-- check if any job is running
	if exists(select job_gid from recon_trn_tjob
		where recon_code = v_recon_code
		and jobtype_code in ('A','M','U','T','UJ')
		and job_status in ('I','P')
    and job_gid <> in_job_gid
		and delete_flag = 'N') then

		select group_concat(cast(job_gid as nchar)) into v_txt from recon_trn_tjob
		where recon_code = v_recon_code
		and jobtype_code in ('A','M','U','T','UJ')
		and job_status in ('I','P')
    and job_gid <> in_job_gid
		and delete_flag = 'N';

		set out_msg = concat('KO/Undo KO/Field Update/Theme is already running in the job id ', v_txt ,' ! ');
		set out_result = 0;
		leave me;
	end if;

  drop temporary table if exists recon_tmp_t1recon;
  drop temporary table if exists recon_tmp_t1theme;

  CREATE TEMPORARY TABLE recon_tmp_t1recon(
    recon_code varchar(32) not null,
    PRIMARY KEY (recon_code)
  ) ENGINE = MyISAM;

  CREATE TEMPORARY TABLE recon_tmp_t1theme(
    recon_code varchar(32) not null,
    theme_desc text not null,
    PRIMARY KEY (recon_code,theme_desc(255))
  ) ENGINE = MyISAM;

  -- get recon code
  insert into recon_tmp_t1recon (recon_code)
  select
    distinct recon_code
  from recon_trn_tthemeupdate
  where scheduler_gid = in_scheduler_gid
  and recon_code <> ''
  and delete_flag = 'N';

  -- concurrent KO flag
  set v_concurrent_ko_flag = fn_get_configvalue('concurrent_ko_flag');

	-- recon block
	recon_block:begin
		declare recon_done int default 0;
		declare recon_cursor cursor for
		  select a.recon_code,b.recon_theme_flag from recon_tmp_t1recon as a
      inner join recon_mst_trecon as b on a.recon_code = b.recon_code
        and b.active_status = 'Y' and b.delete_flag = 'N';
		declare continue handler for not found set recon_done=1;

		open recon_cursor;

		recon_loop: loop
			fetch recon_cursor into v_recon_code,v_recon_theme_flag;
			if recon_done = 1 then leave recon_loop; end if;

			if v_concurrent_ko_flag = 'Y' then
				set v_tran_table = concat(v_recon_code,'_tran');
				set v_tranbrkp_table = concat(v_recon_code,'_tranbrkp');
			else
				set v_tran_table = 'recon_trn_ttran';
				set v_tranbrkp_table = 'recon_trn_ttranbrkp';
			end if;

      if v_recon_theme_flag = 'Y' then
        truncate recon_tmp_t1theme;

        -- insert recon theme
        insert into recon_tmp_t1theme(recon_code,theme_desc)
        select
          v_recon_code,manualtheme_desc
        from recon_mst_tmanualtheme
        where recon_code = v_recon_code
        and active_status = 'Y'
        and delete_flag = 'N';

        -- set failure for all lines
        update recon_trn_tthemeupdate set
          theme_status = 'F',
          theme_remark = 'Theme not maintained in the master'
        where scheduler_gid = in_scheduler_gid
        and recon_code = v_recon_code
        and theme_status = 'P'
        and delete_flag = 'N';

        -- set theme valudation for valid theme desc
        update recon_trn_tthemeupdate as a
        inner join recon_tmp_t1theme as b on a.recon_code = b.recon_code
          and a.theme_desc = b.theme_desc
        set
          a.theme_status = 'P',
          a.theme_remark = ''
        where a.scheduler_gid = in_scheduler_gid
        and a.recon_code = v_recon_code
        and a.theme_status = 'F'
        and a.delete_flag = 'N';
      end if;

			-- update the recon_trn_ttran
			set v_sql = concat("
				update recon_trn_tthemeupdate as a
				inner join ",v_tran_table," as b on a.tran_gid = b.tran_gid and b.delete_flag = 'N'
				set b.theme_code = a.theme_desc,
					a.theme_status = 'C'
				where a.scheduler_gid = ",cast(in_scheduler_gid as nchar),"
        and a.recon_code = '",v_recon_code,"'
				and a.theme_status = 'P'
				and a.tranbrkp_gid = 0
				and a.delete_flag = 'N'");

			call pr_run_sql2(v_sql,@msg2,@result2);

			-- update the recon_trn_ttranbrkp
			set v_sql = concat("
				update recon_trn_tthemeupdate as a
				inner join ",v_tranbrkp_table," as b on a.tranbrkp_gid = b.tranbrkp_gid and b.delete_flag = 'N'
				set b.theme_code = a.theme_desc,
					a.theme_status = 'C'
				where a.scheduler_gid = ",cast(in_scheduler_gid as nchar),"
        and a.recon_code = '",v_recon_code,"'
				and a.theme_status = 'P'
				and a.delete_flag = 'N'");

			call pr_run_sql2(v_sql,@msg2,@result2);
		end loop recon_loop;
		close recon_cursor;
	end recon_block;

  -- update failed cases
  update recon_trn_tthemeupdate
    set theme_status = 'F'
  where scheduler_gid = in_scheduler_gid
  and theme_status = 'P'
  and delete_flag = 'N';

  drop temporary table if exists recon_tmp_t1recon;
  drop temporary table if exists recon_tmp_t1theme;

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;