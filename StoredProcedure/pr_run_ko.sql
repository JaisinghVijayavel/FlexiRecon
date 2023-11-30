DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_ko` $$
CREATE PROCEDURE `pr_run_ko`(
  in in_recon_code text,
  in in_period_from date,
  in in_period_to date,
  in in_automatch_flag char(1),
  in in_ip_addr varchar(255),
  in in_user_code varchar(16),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare i int default 0;

  declare v_txt_rule_code text default '';
  declare v_rule_code text default '';
  declare v_rule_name text default '';

  declare v_txt_recon_code text default '';
  declare v_recon_code text default '';
  declare v_recon_gid int default 0;

  declare v_txt text default '';
  declare v_total_count int default 0;
  declare v_count int default 0;
  declare v_job_gid int default 0;
  declare v_job_input_param text default '';
  declare v_date_format text default '';

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

    call pr_upd_job(v_job_gid,'F',@full_error,@msg,@result);

    set out_msg = @full_error;
    set out_result = 0;

    SIGNAL SQLSTATE '99999' SET
    MYSQL_ERRNO = @errno,
    MESSAGE_TEXT = @text;
  END;
  */

  set v_date_format = fn_get_configvalue('web_date_format');
  set v_recon_code = SPLIT(in_recon_code,'$',1);

  if in_automatch_flag = 'Y' then
    if exists(select job_gid from recon_trn_tjob
      where jobtype_code = 'A'
      and job_status in ('I','P')
      and delete_flag = 'N') then

      select group_concat(cast(job_gid as nchar)) into v_txt from recon_trn_tjob
      where jobtype_code = 'A'
      and job_status in ('I','P')
      and delete_flag = 'N';

      set out_msg = concat('Rule Based KO - Automatic is already running in the job id ', v_txt ,' ! ');
      set out_result = 0;

      set v_job_gid = 0;

      SIGNAL SQLSTATE '99999' SET
      MYSQL_ERRNO = 9999,
      MESSAGE_TEXT = out_msg;

      leave me;
    else
      call pr_ins_job(v_recon_code,'A',0,'Auto match','',in_user_code,in_ip_addr,'I','Initiated...',@out_job_gid,@msg,@result);
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

  drop temporary table if exists tb_recon;
  create temporary table if not exists tb_recon
  (
    recon_code nvarchar(6500)
  );

  call pr_get_split(in_recon_code,',',false);
  insert into tb_recon(recon_code) select item from tb_search;

  recon_block:begin
    declare recon_done int default 0;
    declare recon_cursor cursor for
      select recon_code from tb_recon;
    declare continue handler for not found set recon_done=1;

    open recon_cursor;

    recon_loop: loop
      fetch recon_cursor into v_txt_recon_code;

      if recon_done = 1 then leave recon_loop; end if;
      set v_txt_recon_code = ifnull(v_txt_recon_code,'');

      if v_txt_recon_code <> '' then
        set i = instr(v_txt_recon_code,'$');

        if i > 0 then
          set v_txt = substr(v_txt_recon_code,1,i-1);
          set v_txt_rule_code = substr(v_txt_recon_code,i+1);
        else
          set v_txt = v_txt_recon_code;
          set v_txt_rule_code = '';
        end if;

        set v_recon_code = v_txt;

        select
          recon_gid into v_recon_gid
        from recon_mst_trecon
        where recon_code = v_recon_code
        and active_status = 'Y'
        and period_from <= curdate()
        and (period_to >= curdate()
        or until_active_flag = 'Y')
        and delete_flag = 'N';

        set v_recon_gid = ifnull(v_recon_gid,0);

        if v_recon_gid > 0 then
          /*
          if in_automatch_flag = 'Y' then
            select count(*) into v_count from recon_trn_ttran as a
            where a.recon_code = v_recon_code
            and a.excp_value > 0
            and a.tran_date >= in_period_from
            and a.tran_date <= in_period_to
            and a.mapped_value = 0
            and a.delete_flag = 'N';

            set v_total_count = v_total_count + ifnull(v_count,0);

            select count(*) into v_count from recon_trn_ttranbrkp as a
            where a.recon_code = v_recon_code
            and a.tran_date >= in_period_from
            and a.tran_date <= in_period_to
            and a.excp_value > 0
            and a.tran_gid > 0
            and a.delete_flag = 'N';

            set v_total_count = v_total_count + ifnull(v_count,0);
          end if;
          */

          rule_loop: loop
            set i = instr(v_txt_rule_code,'#');

            if i = 0 then
              if v_txt_rule_code = '' then
                set v_rule_code = '';
              else
                set v_rule_code = v_txt_rule_code;
                set v_txt_rule_code = '';
              end if;
            else
              set v_txt = substr(v_txt_rule_code,1,i-1);
              set v_txt_rule_code = substr(v_txt_rule_code,i+1);

              set v_rule_code = v_txt;

              if v_rule_name = '' then
                set v_rule_name = concat('Rule : ',fn_get_rulename(v_rule_code));
              else
                set v_rule_name = concat(v_rule_name,',',fn_get_rulename(v_rule_code));
              end if;
            end if;

            call pr_run_automatchwithbrkp_partial(v_recon_code,v_rule_code,v_job_gid,in_period_from,in_period_to,in_automatch_flag,in_user_code,@msg,@result);

            if v_txt_rule_code = '' then leave rule_loop; end if;
          end loop rule_loop;
        end if;
      end if;
    end loop recon_loop;

    close recon_cursor;
  end recon_block;

  drop temporary table if exists tb_recon;


  if v_rule_name <> '' then
    set v_job_input_param = v_rule_name;
  end if;

  set v_job_input_param = concat(char(13),char(10),v_job_input_param,'Period From : ',date_format(in_period_from,v_date_format));
  set v_job_input_param = concat(char(13),char(10),v_job_input_param,'Period To : ',date_format(in_period_to,v_date_format));

  call pr_upd_jobwithparam(v_job_gid,v_job_input_param,'C','Completed',@msg,@result);

  if in_automatch_flag = 'N' then
    call pr_run_previewreport(v_job_gid,0,in_user_code,@msg,@result);

    call pr_get_tablequery(v_recon_code,'recon_rpt_tpreview',concat('and job_gid = ',cast(v_job_gid as nchar),' '),v_job_gid,
                                 in_user_code,@msg,@result);
  end if;

  set out_result = 1;
  set out_msg = 'Success';
end $$

DELIMITER ;