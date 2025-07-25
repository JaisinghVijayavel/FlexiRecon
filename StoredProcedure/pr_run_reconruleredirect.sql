﻿DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_reconruleredirect` $$
CREATE PROCEDURE `pr_run_reconruleredirect`(
  in in_recon_code text,
  in in_period_from date,
  in in_period_to date,
  in in_automatch_flag char(1),
  in in_ip_addr varchar(255),
  in in_user_code varchar(32),
  out out_job_gid int,
  out out_msg text,
  out out_result int
)
me:BEGIN
/*
    Created By : Muthu
    Created Date - 2025-02-19

    Updated By : Vijayavel
    updated Date : 11-04-2025

	  Version - 7
*/

  declare i int default 0;

  declare v_recon_name text default '';
  declare v_recon_date_flag text default '';
  declare v_txt_rule_code text default '';
  declare v_rule_code text default '';
  declare v_rule_name text default '';
  declare v_rule_apply_on text default '';
  declare v_system_match_flag text default '';
  declare v_probable_match_flag text default '';
  declare v_group_flag text default '';

  declare v_txt_recon_code text default '';
  declare v_recon_code text default '';
  declare v_recontype_code text default '';
  declare v_recon_processing_method text default '';
  declare v_recon_rule_version text default '';
  declare v_recon_date_field text default '';
  declare v_recon_gid int default 0;

  declare v_tran_table text default '';
  declare v_tranbrkp_table text default '';

  declare v_txt text default '';
  declare v_sql text default '';

  declare v_recon_date_condition text default '';

  declare v_total_count int default 0;
  declare v_count int default 0;
  declare v_job_gid int default 0;
  declare v_job_input_param text default '';
  declare v_date_format text default '';

  declare v_file_name text default '';

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  declare v_concurrent_ko_flag text default '';
  declare v_koseq_flag text default '';

  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE,
    @errno = MYSQL_ERRNO, @text = MESSAGE_TEXT;

    SET @full_error = CONCAT("ERROR ", @errno, " (", @sqlstate, "): ", @text,' ',err_msg);

    ROLLBACK;

    call pr_upd_job(v_job_gid,'F',@full_error,@msg,@result);

    set out_msg = @full_error;
    set out_result = 0;

    SIGNAL SQLSTATE '99999' SET
    MYSQL_ERRNO = @errno,
    MESSAGE_TEXT = @text;
  END;

  set v_date_format = fn_get_configvalue('web_date_format');
  set v_recon_code = SPLIT(in_recon_code,'$',1);

  if not exists(select recon_code from recon_mst_trecon
    where recon_code = in_recon_code
    and active_status = 'Y'
    and period_from <= curdate()
    and (period_to >= curdate()
    or until_active_flag = 'Y')
    and delete_flag = 'N') then

    set out_msg = 'Invalid recon !';
    set out_result = 0;

    leave me;
  else
    select
      recon_rule_version into v_recon_rule_version
    from recon_mst_trecon
    where recon_code = in_recon_code
    and delete_flag = 'N';

    set v_recon_rule_version = ifnull(v_recon_rule_version,'');
  end if;

  -- check ko sequence
  if exists(select * from recon_mst_tkoseq
    where recon_code = in_recon_code
    and active_status = 'Y'
    and hold_flag = 'N'
    and delete_flag = 'N') then
    set v_koseq_flag = 'Y';
  else
    set v_koseq_flag = 'N';
  end if;

  -- get recon details
  select
    recon_name,
    recontype_code,
    processing_method,
    recon_date_field,
    recon_date_flag
  into
    v_recon_name,
    v_recontype_code,
    v_recon_processing_method,
    v_recon_date_field,
    v_recon_date_flag
  from recon_mst_trecon
  where recon_code = in_recon_code
  and period_from <= curdate()
  and (until_active_flag = 'Y'
  or period_to >= curdate())
  and delete_flag = 'N';

  set v_recon_name = ifnull(v_recon_name,'');
  set v_recontype_code = ifnull(v_recontype_code,'');
  set v_recon_date_field = ifnull(v_recon_date_field,'');
  set v_recon_date_flag = ifnull(v_recon_date_flag,'N');
  set v_recon_processing_method = ifnull(v_recon_processing_method,'S');

  if v_recon_processing_method = 'S' then
    set v_koseq_flag = 'N';
  end if;

  if v_recon_date_flag = 'Y' then
    set v_recon_date_condition = concat(v_recon_date_condition,' and ',v_recon_date_field,' >= ');
    set v_recon_date_condition = concat(v_recon_date_condition,char(39),date_format(in_period_from,'%Y-%m-%d'),char(39),' ');
    set v_recon_date_condition = concat(v_recon_date_condition,' and ',v_recon_date_field,' <= ');
    set v_recon_date_condition = concat(v_recon_date_condition,char(39),date_format(in_period_to,'%Y-%m-%d'),char(39),' ');
  end if;

  if in_automatch_flag = 'Y' then
    if exists(select job_gid from recon_trn_tjob
      where recon_code = in_recon_code
      and jobtype_code in ('A','M','U','T','UJ')
      and job_status in ('I','P')
      and delete_flag = 'N') then

      select group_concat(cast(job_gid as nchar)) into v_txt from recon_trn_tjob
      where recon_code = in_recon_code
      and jobtype_code in ('A','M','U','T','UJ')
      and job_status in ('I','P')
      and delete_flag = 'N';

      set out_msg = concat('Automatic/Manual/Undo Job/Theme is already running in the job id ', v_txt ,' ! ');
      set out_result = 0;

      set v_job_gid = 0;

      leave me;
    else
      call pr_ins_job(v_recon_code,'A',0,'Auto/Probable match','',in_user_code,in_ip_addr,'I','Initiated...',@out_job_gid,@msg,@result);
    end if;
  else
    call pr_ins_job(v_recon_code,'P',0,'Preview Auto Match','',in_user_code,in_ip_addr,'I','Initiated...',@out_job_gid,@msg,@result);
  end if;

  if @result = 0 then
    set out_msg = @msg;
    set out_result = 0;

    leave me;
  end if;

  set v_job_gid = @out_job_gid;

  if v_recontype_code = 'W' or v_recontype_code = 'B' then
    if fn_get_chkbalance(in_recon_code,in_period_to) = false then
      set out_msg = 'Recon was not tallied !';
      set out_result = 0;

      SIGNAL SQLSTATE '99999' SET MESSAGE_TEXT = 'Recon was not tallied';
      leave me;
    end if;
  end if;

  delete from recon_trn_tdatasetjob
  where recon_code = in_recon_code
  and dataset_code in
  (
    select dataset_code from recon_mst_trecondataset
    where recon_code = in_recon_code
    and delete_flag = 'N'
  )
  and automatch_flag = in_automatch_flag
  and delete_flag = 'N';

  -- concurrent KO flag
  set v_concurrent_ko_flag = fn_get_configvalue('concurrent_ko_flag');

  if v_concurrent_ko_flag = 'Y' then
    set v_tran_table = concat(v_recon_code,'_tran');
    set v_tranbrkp_table = concat(v_recon_code,'_tranbrkp');
  else
    set v_tran_table = 'recon_trn_ttran';
    set v_tranbrkp_table = 'recon_trn_ttranbrkp';
  end if;

  /*
	set v_sql = 'update $TABLENAME$ set ';
	set v_sql = concat(v_sql,'theme_code = '''' ');
	set v_sql = concat(v_sql,'where recon_code = ',char(39),in_recon_code,char(39),' ');
	set v_sql = concat(v_sql,v_recon_date_condition);
	set v_sql = concat(v_sql,'and delete_flag = ',char(39),'N',char(39),' ');

	call pr_run_sql(replace(v_sql,'$TABLENAME$',v_tran_table),@msg,@result);
	call pr_run_sql(replace(v_sql,'$TABLENAME$',v_tranbrkp_table),@msg,@result);
  */

  if v_koseq_flag = 'N' then
    call pr_run_preprocess(in_recon_code,'',v_job_gid,'N',in_period_from,in_period_to,in_automatch_flag,@msg,@result);
  end if;

  drop temporary table if exists recon_tmp_ttran;
  drop temporary table if exists recon_tmp_ttranbrkp;

  create temporary table recon_tmp_ttran select * from recon_trn_ttran where 1 = 2;
  alter table recon_tmp_ttran add primary key(tran_gid);
  create index idx_recon_code on recon_tmp_ttran(recon_code);
  create index idx_excp_value on recon_tmp_ttran(recon_code,dataset_code,excp_value);
  create index idx_tran_date on recon_tmp_ttran(tran_date);
  create index idx_dataset_code on recon_tmp_ttran(recon_code,dataset_code,tran_acc_mode);
  alter table recon_tmp_ttran ENGINE = MyISAM;

  create temporary table recon_tmp_ttranbrkp select * from recon_trn_ttranbrkp where 1 = 2;
  alter table recon_tmp_ttranbrkp add primary key(tranbrkp_gid);
  create index idx_recon_code on recon_tmp_ttranbrkp(recon_code);
  create index idx_excp_value on recon_tmp_ttranbrkp(recon_code,dataset_code,excp_value);
  create index idx_tran_date on recon_tmp_ttranbrkp(tran_date);
  create index idx_tran_gid on recon_tmp_ttranbrkp(tran_gid);
  create index idx_dataset_code on recon_tmp_ttranbrkp(recon_code,dataset_code,tran_acc_mode);
  alter table recon_tmp_ttranbrkp ENGINE = MyISAM;

  if in_automatch_flag = 'N' then
    if v_recon_date_flag = 'Y' then
      set v_sql = concat("insert into recon_tmp_ttran
        select * from ",v_tran_table,"
        where recon_code = '",in_recon_code,"'
        and tran_date >= '",cast(in_period_from as nchar),"'
        and tran_date <= '",cast(in_period_to as nchar),"'
        and delete_flag = 'N'");

      call pr_run_sql1(v_sql,@msg1,@result1);

      set v_sql = concat("insert into recon_tmp_ttranbrkp
        select * from ",v_tranbrkp_table,"
        where recon_code = '",in_recon_code,"'
        and tran_date >= '",cast(in_period_from as nchar),"'
        and tran_date <= '",cast(in_period_to as nchar),"'
        and delete_flag = 'N'");

      call pr_run_sql1(v_sql,@msg1,@result1);
    else
      set v_sql = concat("insert into recon_tmp_ttran
        select * from ",v_tran_table,"
        where recon_code = '",in_recon_code,"'
        and delete_flag = 'N'");

      call pr_run_sql1(v_sql,@msg1,@result1);

      set v_sql = concat("insert into recon_tmp_ttranbrkp
        select * from ",v_tranbrkp_table,"
        where recon_code = '",in_recon_code,"'
        and delete_flag = 'N'");

      call pr_run_sql1(v_sql,@msg1,@result1);
    end if;

    set v_tran_table = 'recon_tmp_ttran';
    set v_tranbrkp_table = 'recon_tmp_ttranbrkp';
  end if;

	set v_sql = concat("
	insert into recon_trn_tdatasetjob
	(
		recon_code,
		dataset_code,
		automatch_flag,
		job_gid,
		before_dr_count,
		before_dr_value,
		before_cr_count,
		before_cr_value,
		before_count,
		before_value,
		insert_date,
		insert_by
	)
	select
		recon_code,
		tranbrkp_dataset_code,
		'",in_automatch_flag,"',
		",cast(v_job_gid as nchar),",
		sum(if(tran_acc_mode = 'D',1,0)) as dr_count,
		sum(if(tran_acc_mode = 'D',excp_value,0)) as dr_value,
		sum(if(tran_acc_mode = 'C',1,0)) as cr_count,
		sum(if(tran_acc_mode = 'C',excp_value,0)) as cr_value,
		count(*),
		sum(excp_value),
		sysdate(),
		'",in_user_code,"'
	from ",v_tranbrkp_table,"
	where recon_code = '",in_recon_code,"'
	and excp_value > 0
	and delete_flag = 'N'
	group by recon_code,tranbrkp_dataset_code");

	call pr_run_sql1(v_sql,@msg1,@result1);

	set v_sql = concat("
	insert into recon_trn_tdatasetjob
	(
		recon_code,
		dataset_code,
		automatch_flag,
		job_gid,
		before_dr_count,
		before_dr_value,
		before_cr_count,
		before_cr_value,
		before_count,
		before_value,
		insert_date,
		insert_by
	)
	select
		recon_code,
		dataset_code,
		'",in_automatch_flag,"',
		",cast(v_job_gid as nchar),",
		sum(if(tran_acc_mode = 'D',1,0)) as dr_count,
		sum(if(tran_acc_mode = 'D',excp_value,0)) as dr_value,
		sum(if(tran_acc_mode = 'C',1,0)) as cr_count,
		sum(if(tran_acc_mode = 'C',excp_value,0)) as cr_value,
		count(*),
		sum(excp_value),
		sysdate(),
		'",in_user_code,"'
	from ",v_tran_table,"
	where recon_code = '",in_recon_code,"'
	and excp_value > 0
	and delete_flag = 'N'
	group by recon_code,dataset_code");

	call pr_run_sql1(v_sql,@msg1,@result1);

	if v_koseq_flag = 'N' then
		rule_block:begin
			declare rule_done int default 0;
			declare rule_cursor cursor for
				select
					rule_code,
					rule_apply_on,
					group_flag,
					system_match_flag,
					probable_match_flag
				from recon_mst_trulehistory
				where recon_code = in_recon_code
        and recon_version = v_recon_rule_version
				and hold_flag = 'N'
				and active_status = 'Y'
				and period_from <= curdate()
				and (period_to >= curdate()
				or until_active_flag = 'Y')
				and delete_flag = 'N'
				order by rule_order;
			declare continue handler for not found set rule_done=1;

			open rule_cursor;

			rule_loop: loop
				fetch rule_cursor into v_rule_code,v_rule_apply_on,v_group_flag,v_system_match_flag,v_probable_match_flag;

				if rule_done = 1 then leave rule_loop; end if;

				set v_rule_code = ifnull(v_rule_code,'');
				set v_rule_apply_on = ifnull(v_rule_apply_on,'');

				set v_system_match_flag = ifnull(v_system_match_flag,'N');
				set v_probable_match_flag = ifnull(v_probable_match_flag,'N');

				if in_automatch_flag = 'Y' then
					if v_probable_match_flag = 'Y' then
						set v_system_match_flag = 'N';

						truncate recon_tmp_ttran;
						truncate recon_tmp_ttranbrkp;

						set v_sql = concat("
							insert into recon_tmp_ttran
							select * from ",v_tran_table,"
							where recon_code = '",in_recon_code,"'
							and delete_flag = 'N'");

						call pr_run_sql1(v_sql,@msg1,@result1);

						set v_sql = concat("
							insert into recon_tmp_ttranbrkp
							select * from ",v_tranbrkp_table,"
							where recon_code = '",in_recon_code,"'
							and delete_flag = 'N'");

						call pr_run_sql1(v_sql,@msg1,@result1);
					end if;
				else
					set v_system_match_flag = 'N';
				end if;

				if v_rule_apply_on = 'T' then
					call pr_run_automatch(v_recon_code,v_rule_code,v_group_flag,v_job_gid,in_period_from,in_period_to,v_system_match_flag,in_user_code,@msg,@result);
					-- call pr_run_automatch_partial(v_recon_code,v_rule_code,v_group_flag,v_job_gid,in_period_from,in_period_to,v_system_match_flag,in_user_code,@msg,@result);
					call pr_run_automatch_partial_new(v_recon_code,v_rule_code,v_group_flag,v_job_gid,in_period_from,in_period_to,v_system_match_flag,in_user_code,@msg,@result);

					if v_group_flag = 'MTM' then
						set v_group_flag = 'OTM';

						call pr_run_automatch(v_recon_code,v_rule_code,v_group_flag,v_job_gid,in_period_from,in_period_to,v_system_match_flag,in_user_code,@msg,@result);
						-- call pr_run_automatch_partial(v_recon_code,v_group_flag,v_rule_code,v_job_gid,in_period_from,in_period_to,v_system_match_flag,in_user_code,@msg,@result);
						call pr_run_automatch_partial_new(v_recon_code,v_group_flag,v_rule_code,v_job_gid,in_period_from,in_period_to,v_system_match_flag,in_user_code,@msg,@result);
					end if;
				elseif v_rule_apply_on = 'S' then
					call pr_run_posttranbrkprule(v_recon_code,v_rule_code,v_job_gid,in_period_from,in_period_to,v_system_match_flag,in_user_code,@msg,@result);
				end if;
			end loop rule_loop;

			close rule_cursor;
		end rule_block;
	else
		-- koseq block
		koseq_block:begin
			declare koseq_done int default 0;
			declare v_koseq_type varchar(32);
			declare v_koseq_ref_code varchar(32);

			declare koseq_cursor cursor for
			select koseq_type,koseq_ref_code from recon_mst_tkoseq
				where recon_code = in_recon_code
				and active_status = 'Y'
				and hold_flag = 'N'
				and delete_flag = 'N'
				order by koseq_no;

			declare continue handler for not found set koseq_done=1;

			open koseq_cursor;

			koseq_loop: loop
				fetch koseq_cursor into v_koseq_type,v_koseq_ref_code;
				if koseq_done = 1 then leave koseq_loop; end if;

				if v_koseq_type = 'Preprocess' or koseq_type = 'Postprocess' or koseq_type = 'Process' then
					call pr_run_preprocess(in_recon_code,v_koseq_ref_code,v_job_gid,'N',in_period_from,in_period_to,in_automatch_flag,@msg,@result);
				elseif v_koseq_type = 'Theme' then
					call pr_run_theme(v_recon_code,v_koseq_ref_code,v_job_gid,in_period_from,in_period_to,
						in_automatch_flag,in_ip_addr,in_user_code,@msg,@result);
				elseif v_koseq_type = 'Rule' then
					select
						rule_code,
						rule_apply_on,
						group_flag,
						system_match_flag,
						probable_match_flag
					into
						v_rule_code,
						v_rule_apply_on,
						v_group_flag,
						v_system_match_flag,
						v_probable_match_flag
					from recon_mst_trulehistory
					where recon_code = in_recon_code
					and rule_code = v_koseq_ref_code
          and recon_version = v_recon_rule_version
					and hold_flag = 'N'
					and active_status = 'Y'
					and period_from <= curdate()
					and (period_to >= curdate()
					or until_active_flag = 'Y')
					and delete_flag = 'N';

					set v_rule_code = ifnull(v_rule_code,'');
					set v_rule_apply_on = ifnull(v_rule_apply_on,'');

					set v_system_match_flag = ifnull(v_system_match_flag,'N');
					set v_probable_match_flag = ifnull(v_probable_match_flag,'N');

					if in_automatch_flag = 'Y' then
						if v_probable_match_flag = 'Y' then
							set v_system_match_flag = 'N';

							truncate recon_tmp_ttran;
							truncate recon_tmp_ttranbrkp;

							set v_sql = concat("
								insert into recon_tmp_ttran
								select * from ",v_tran_table,"
								where recon_code = '",in_recon_code,"'
								and delete_flag = 'N'");

							call pr_run_sql1(v_sql,@msg1,@result1);

							set v_sql = concat("
								insert into recon_tmp_ttranbrkp
								select * from ",v_tranbrkp_table,"
								where recon_code = '",in_recon_code,"'
								and delete_flag = 'N'");

							call pr_run_sql1(v_sql,@msg1,@result1);
						end if;
					else
						set v_system_match_flag = 'N';
					end if;

					if v_rule_apply_on = 'T' then
						call pr_run_automatch(v_recon_code,v_rule_code,v_group_flag,v_job_gid,in_period_from,in_period_to,v_system_match_flag,in_user_code,@msg,@result);
						-- call pr_run_automatch_partial(v_recon_code,v_rule_code,v_group_flag,v_job_gid,in_period_from,in_period_to,v_system_match_flag,in_user_code,@msg,@result);
						call pr_run_automatch_partial_new(v_recon_code,v_rule_code,v_group_flag,v_job_gid,in_period_from,in_period_to,v_system_match_flag,in_user_code,@msg,@result);

						if v_group_flag = 'MTM' then
							set v_group_flag = 'OTM';

							call pr_run_automatch(v_recon_code,v_rule_code,v_group_flag,v_job_gid,in_period_from,in_period_to,v_system_match_flag,in_user_code,@msg,@result);
							-- call pr_run_automatch_partial(v_recon_code,v_group_flag,v_rule_code,v_job_gid,in_period_from,in_period_to,v_system_match_flag,in_user_code,@msg,@result);
							call pr_run_automatch_partial_new(v_recon_code,v_group_flag,v_rule_code,v_job_gid,in_period_from,in_period_to,v_system_match_flag,in_user_code,@msg,@result);
						end if;
					elseif v_rule_apply_on = 'S' then
						call pr_run_posttranbrkprule(v_recon_code,v_rule_code,v_job_gid,in_period_from,in_period_to,v_system_match_flag,in_user_code,@msg,@result);
					end if;
				end if;

			end loop koseq_loop;
			close koseq_cursor;
		end koseq_block;
	end if;
	
  set v_job_input_param = concat(v_job_input_param,'Period From : ',date_format(in_period_from,v_date_format),char(13),char(10));
  set v_job_input_param = concat(v_job_input_param,'Period To : ',date_format(in_period_to,v_date_format),char(13),char(10));

  if in_automatch_flag = 'N' then
    call pr_run_previewreport(in_recon_code,v_job_gid,0,in_user_code,@msg,@result);

    set v_file_name = concat(cast(v_job_gid as nchar),'_',in_recon_code,'_MatchPreview.csv');

    call pr_get_tablequery(v_recon_code,v_file_name,'recon_rpt_tpreview',concat('and job_gid = ',cast(v_job_gid as nchar),' '),v_job_gid,
                                 in_user_code,@msg,@result);
  elseif in_automatch_flag = 'Y' then
    update recon_mst_trecon set
      last_job_gid = v_job_gid
    where recon_code = v_recon_code
    and delete_flag = 'N';

    call pr_run_previewreport(in_recon_code,v_job_gid,0,in_user_code,@msg,@result);

    set v_file_name = concat(cast(v_job_gid as nchar),'_',in_recon_code,'_ProbableMatchPreview.csv');

    call pr_get_tablequery(v_recon_code,v_file_name,'recon_rpt_tpreview',concat('and job_gid = ',cast(v_job_gid as nchar),' '),v_job_gid,
                                 in_user_code,@msg,@result);

  end if;

    set v_sql = concat("
    update recon_trn_tdatasetjob as a
    inner join
    (
      select
        recon_code,
        tranbrkp_dataset_code as dataset_code,
        sum(if(tran_acc_mode = 'D',1,0)) as dr_count,
        sum(if(tran_acc_mode = 'D',excp_value,0)) as dr_value,
        sum(if(tran_acc_mode = 'C',1,0)) as cr_count,
        sum(if(tran_acc_mode = 'C',excp_value,0)) as cr_value,
        count(*) as rec_count,
        sum(excp_value) as rec_value
      from ",v_tranbrkp_table,"
      where recon_code = '",in_recon_code,"'
      and excp_value > 0
      and delete_flag = 'N'
      group by recon_code,tranbrkp_dataset_code
    ) as b on a.recon_code = b.recon_code and a.dataset_code = b.dataset_code
    set
      a.after_dr_count = b.dr_count,
      a.after_dr_value = b.dr_value,
      a.after_cr_count = b.cr_count,
      a.after_cr_value = b.cr_value,
      a.after_count = b.rec_count,
      a.after_value = b.rec_value,
      a.update_date = sysdate(),
      a.update_by = '",in_user_code,"'
    where a.job_gid = ",cast(v_job_gid as nchar),"
    and a.delete_flag = 'N'");

    call pr_run_sql1(v_sql,@msg1,@result1);

    set v_sql = concat("
    update recon_trn_tdatasetjob as a
    inner join
    (
      select
        recon_code,
        dataset_code,
        sum(if(tran_acc_mode = 'D',1,0)) as dr_count,
        sum(if(tran_acc_mode = 'D',excp_value,0)) as dr_value,
        sum(if(tran_acc_mode = 'C',1,0)) as cr_count,
        sum(if(tran_acc_mode = 'C',excp_value,0)) as cr_value,
        count(*) as rec_count,
        sum(excp_value) as rec_value
      from ",v_tran_table,"
      where recon_code = '",in_recon_code,"'
      and excp_value > 0
      and delete_flag = 'N'
      group by recon_code,dataset_code
    ) as b on a.recon_code = b.recon_code and a.dataset_code = b.dataset_code
    set
      a.after_dr_count = b.dr_count,
      a.after_dr_value = b.dr_value,
      a.after_cr_count = b.cr_count,
      a.after_cr_value = b.cr_value,
      a.after_count = b.rec_count,
      a.after_value = b.rec_value,
      a.update_date = sysdate(),
      a.update_by = '",in_user_code,"'
    where a.job_gid = ",cast(v_job_gid as nchar),"
    and a.delete_flag = 'N'");

    call pr_run_sql1(v_sql,@msg1,@result1);

  set out_result = 1;
  set out_msg = 'Success';

  drop temporary table if exists recon_tmp_ttran;
  drop temporary table if exists recon_tmp_ttranbrkp;

  if in_automatch_flag = 'Y' then
    if v_koseq_flag = 'N' or v_recon_processing_method = 'S' then
      call pr_run_theme(v_recon_code,'',v_job_gid,in_period_from,in_period_to,
        in_automatch_flag,in_ip_addr,in_user_code,@msg,@result);
    end if;

    call pr_run_dynamicreport('','',v_recon_code,'RPT_EXCP_WITHBRKP','','',false,'table',
      in_ip_addr,in_user_code,@msg,@result);

    call pr_run_tablequery('',
                           v_recon_code,
                           'RPT_AMT_MATCHED',
                           'recon_rpt_tpreview',
                           concat(' and job_gid = ',cast(v_job_gid as nchar)),
                           0,
                           false,
                           'table',
                           in_user_code,@msg,@result);
  end if;


  set v_txt = concat('Rule version applied : ',v_recon_rule_version);

	call pr_upd_jobwithparam(v_job_gid,v_job_input_param,'C',v_txt,@msg,@result);
	set out_job_gid = v_job_gid;

end $$

DELIMITER ;