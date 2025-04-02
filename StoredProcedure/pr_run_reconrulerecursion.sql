DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_reconrulerecursion` $$

CREATE PROCEDURE `pr_run_reconrulerecursion`
(
  in in_recon_code text,
  in in_period_from date,
  in in_period_to date,
  in in_automatch_flag char(1),
  in in_ip_addr varchar(255),
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
BEGIN
  /*
    Created By : Muthu
    Created Date : 19-02-2025

    Updated By : Vijayavel
    updated Date : 26-03-2025

    Version : 2
  */

	declare v_opening_count integer default 0;
	declare v_opening_value decimal(18,2) default 0;
	declare v_current_count integer default 0;
	declare v_current_value decimal(18,2) default 0;
	declare v_while_count integer default 0;
	declare v_koqueue_gid integer default 0;
	declare v_out_msg text;
	declare v_out_result int;
	declare v_job_gid integer default 0;
	declare v_job_input_param text;
	declare v_txt text;
	declare v_value_diff decimal(18,2) default 0;
	declare v_count_diff decimal(18,2) default 0;
	declare v_kocount decimal(18,2) default 0;

  declare v_sql text default '';

	declare v_tran_table text default '';
  declare v_concurrent_ko_flag text default '';

  -- concurrent KO flag
  set v_concurrent_ko_flag = fn_get_configvalue('concurrent_ko_flag');

  if v_concurrent_ko_flag = 'Y' then
	  set v_tran_table = concat(in_recon_code,'_tran');
  else
	  set v_tran_table = 'recon_trn_ttran';
  end if;

	set v_sql = concat("
	select
		count(tran_gid) as count,sum(excp_value) as value
	into
		@v_opening_count,@v_opening_value
	from ",v_tran_table,"
	where recon_code = '",in_recon_code,"'
	and delete_flag = 'N'");

	call pr_run_sql2(v_sql,@msg2,@result2);

	set v_opening_count = ifnull(@v_opening_count,0);
	set v_opening_value = ifnull(@v_opening_value,0);

	call pr_run_reconruleredirect(in_recon_code,in_period_from,in_period_to,in_automatch_flag,in_ip_addr,in_user_code,@out_job_gid,@out_msg, @out_result);
	select @out_job_gid,@out_msg,@out_result into v_job_gid,v_out_msg,v_out_result;

	if(v_out_result != 0)then
		set v_sql = concat("
		select
			count(tran_gid) as count,sum(excp_value) as value
		into
			@v_current_count,@v_current_value
		from ",v_tran_table,"
		where recon_code = '",in_recon_code,"'
		and delete_flag = 'N'");

		call pr_run_sql2(v_sql,@msg2,@result2);

    set v_current_count = ifnull(@v_current_count,0);
    set v_current_value = ifnull(@v_current_value,0);

		select
			MAX(koqueue_gid)
		into
			v_koqueue_gid
		from recon_trn_tkoqueue
		where recon_code = in_recon_code
		and (koqueue_status = 'P' || koqueue_status = 'C')
		and delete_flag = 'N';

		if (v_opening_count!=v_current_count || v_opening_value!=v_current_value) then
			UPDATE recon_trn_tkoqueue SET
				koqueue_status = 'I',
				koqueue_remark = concat('Recursive running'),
				start_date = sysdate()
			WHERE koqueue_gid = v_koqueue_gid
			and delete_flag = 'N';

			set v_value_diff = (v_opening_value-v_current_value);
			set v_count_diff = (v_opening_count-v_current_count);

			if( v_count_diff > 0 || v_value_diff > 0)then
				select count(*) into v_kocount from recon_trn_tko
				where job_gid = v_job_gid 
				and recon_code = in_recon_code 
				and delete_flag = 'N';
                            
				select 
					job_input_param,concat(job_remark,CHAR(10),'Iteration Completed - ',v_kocount) 
				into 
					v_job_input_param,v_txt 
				from recon_trn_tjob 
				where job_gid = v_job_gid 
				and delete_flag = 'N';
                            
				call pr_upd_jobwithparam(v_job_gid,v_job_input_param,'R',v_txt,@msg,@result);
			end if;
		else
			call pr_upd_koqueue(v_koqueue_gid,'C',"",@msg,@result);
		end if;
	end if;
END $$

DELIMITER ;