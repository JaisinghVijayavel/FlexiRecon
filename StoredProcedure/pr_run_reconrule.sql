DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_reconrule` $$
CREATE PROCEDURE `pr_run_reconrule`(
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
  declare v_rule_apply_on text default '';

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

  -- create temporary
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

  -- insert exception records
  insert into recon_tmp_ttran select * from recon_trn_ttran where recon_code = in_recon_code and delete_flag = 'N';
  insert into recon_tmp_ttranbrkp select * from recon_trn_ttranbrkp where recon_code = in_recon_code and delete_flag = 'N';

  rule_block:begin
    declare rule_done int default 0;
    declare rule_cursor cursor for
      select rule_code,rule_apply_on from recon_mst_trule
      where recon_code = in_recon_code
      and delete_flag = 'N'
      order by rule_order;
    declare continue handler for not found set rule_done=1;

    open rule_cursor;

    rule_loop: loop
      fetch rule_cursor into v_rule_code,v_rule_apply_on;

      if rule_done = 1 then leave rule_loop; end if;

      set v_rule_code = ifnull(v_rule_code,'');
      set v_rule_apply_on = ifnull(v_rule_apply_on,'');

      if v_rule_apply_on = 'T' then
        call pr_run_automatch(v_recon_code,v_rule_code,v_job_gid,in_period_from,in_period_to,in_automatch_flag,in_user_code,@msg,@result);
        call pr_run_automatch_partial(v_recon_code,v_rule_code,v_job_gid,in_period_from,in_period_to,in_automatch_flag,in_user_code,@msg,@result);
      elseif v_rule_apply_on = 'S' then
        call pr_run_posttranbrkprule(v_recon_code,v_rule_code,v_job_gid,in_period_from,in_period_to,in_automatch_flag,in_user_code,@msg,@result);
      end if;
    end loop rule_loop;

    close rule_cursor;
  end rule_block;

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

  -- drop tempoaray table
  drop temporary table if exists recon_tmp_ttran;
  drop temporary table if exists recon_tmp_ttranbrkp;
end $$

DELIMITER ;