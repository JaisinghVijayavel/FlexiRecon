DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_amountmatchedmultiple` $$
CREATE PROCEDURE `pr_run_amountmatchedmultiple`
(
  in in_recon_code text,
  in in_job_gid int,
  in in_rptsession_gid int,
  in in_condition text,
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:begin

  declare v_sql text default '';
  declare v_out_msg text default '';
  declare v_out_result int default 0;
  declare v_job_gid int default 0;
  declare v_recon_name text default '';
  declare v_tran_field text default '';
  declare v_tranbrkp_field text default '';
  declare v_rpt_path text default '';
  declare v_date_format text default '';
  declare v_recontype_code text default '';
  declare v_user_code varchar(32) default '';

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';

  declare v_concurrent_ko_flag text default '';

  /*
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE,
    @errno = MYSQL_ERRNO, @text = MESSAGE_TEXT;
    SET @full_error = CONCAT("ERROR ", @errno, " (", @sqlstate, "): ", @text);

    ROLLBACK;

    if in_job_gid > 0 then
      call pr_upd_job(in_job_gid,'F',@full_error,@msg,@result);
    end if;

    set out_msg = @full_error;
    set out_result = 0;

    SIGNAL SQLSTATE '99999' SET
    MYSQL_ERRNO = @errno,
    MESSAGE_TEXT = @text;
  END;
  */

  -- concurrent KO flag
  set v_concurrent_ko_flag = fn_get_configvalue('concurrent_ko_flag');

  if v_concurrent_ko_flag = 'Y' then
	  set v_tran_table = concat(in_recon_code,'_tran');
	  set v_tranbrkp_table = concat(in_recon_code,'_tranbrkp');
  else
	  set v_tran_table = 'recon_trn_ttran';
	  set v_tranbrkp_table = 'recon_trn_ttranbrkp';
  end if;

  SELECT
	  group_concat(t.COLUMN_NAME) into v_tran_field
  FROM information_schema.columns as t
  WHERE t.table_schema=database()
  AND t.table_name = 'recon_trn_ttran';

  SELECT
	  group_concat(t.COLUMN_NAME) into v_tranbrkp_field
  FROM information_schema.columns as t
  WHERE t.table_schema=database()
  AND t.table_name = 'recon_trn_ttranbrkp';

  select recontype_code into v_recontype_code from recon_mst_trecon
  where recon_code = in_recon_code
  and active_status = 'Y'
  and delete_flag = 'N';

  set v_recontype_code = ifnull(v_recontype_code,'');

  drop temporary table if exists recon_tmp_ttranvalue;
  drop temporary table if exists recon_tmp_tvaluematched;
  drop temporary table if exists recon_tmp_ttranvaluematched;
  drop temporary table if exists recon_tmp_trecondataset;

  CREATE TEMPORARY TABLE recon_tmp_trecondataset(
    recon_code varchar(32) not null,
    dataset_code varchar(32) not null,
    dataset_type varchar(32) default null,
    PRIMARY KEY (recon_code,dataset_code),
    key idx_dataset_type(recon_code,dataset_type)
  ) ENGINE = MyISAM;

  insert into recon_tmp_trecondataset
    select recon_code,dataset_code,dataset_type from recon_mst_trecondataset
    where recon_code = in_recon_code
    and active_status = 'Y'
    and delete_flag = 'N';

  create temporary table recon_tmp_ttranvalue select * from recon_tmp_ttranwithbrkp where 1 = 2;

  alter table recon_tmp_ttranvalue add primary key(tran_gid,tranbrkp_gid);
  create index idx_excp_value on recon_tmp_ttranvalue(excp_value,tran_acc_mode);
  create index idx_dataset_code on recon_tmp_ttranvalue(recon_code,dataset_code);

  set in_job_gid = ifnull(in_job_gid,0);
  set in_rptsession_gid = ifnull(in_rptsession_gid,0);

  if in_job_gid > 0 then
    if exists(select job_gid from recon_trn_tjob where job_gid = in_job_gid
      and job_status <> 'C' and job_status <> 'F' and delete_flag = 'N') then
      set v_job_gid = in_job_gid;
    else
      set out_result = 0;
      set out_msg = 'Invalid job id !';
      leave me;
    end if;
  end if;

  set v_sql = concat('insert into recon_tmp_ttranvalue(',v_tran_field,') ');
  set v_sql = concat(v_sql,'select z.* from  (');
  set v_sql = concat(v_sql,'select ',v_tran_field,' from ',v_tran_table,' ');
  set v_sql = concat(v_sql,'where true ');
  set v_sql = concat(v_sql,in_condition,' ');
  set v_sql = concat(v_sql,'and excp_value <> 0 ');
  set v_sql = concat(v_sql,'and mapped_value = 0 ');
  set v_sql = concat(v_sql,'and delete_flag = ''N'' LOCK IN SHARE MODE) as z ');

  call pr_run_sql(v_sql,v_out_msg,v_out_result);

  set v_sql = concat('insert into recon_tmp_ttranvalue(',v_tranbrkp_field,') ');
  set v_sql = concat(v_sql,'select z.* from  (');
  set v_sql = concat(v_sql,'select ',v_tranbrkp_field,' from ',v_tranbrkp_table,' ');
  set v_sql = concat(v_sql,'where true ');
  set v_sql = concat(v_sql,in_condition,' ');
  set v_sql = concat(v_sql,'and excp_value > 0 ');
  set v_sql = concat(v_sql,'and tran_gid > 0 ');
  set v_sql = concat(v_sql,'and delete_flag = ''N'' LOCK IN SHARE MODE) as z ');

  call pr_run_sql(v_sql,v_out_msg,v_out_result);

  select job_initiated_by into v_user_code from recon_trn_tjob
  where job_gid = in_job_gid
  and delete_flag = 'N';

  create temporary table recon_tmp_tvaluematched(
    sno int unsigned not null auto_increment,
    tran_value double(15,2) not null default 0,
    rec_count int not null default 0,
    primary key(sno),
    key idx_tran_value(tran_value)
  );

  create temporary table recon_tmp_ttranvaluematched(
    gid int unsigned not null auto_increment,
    sno int not null default 0,
    sub_sno int not null default 0,
    tran_gid int not null default 0,
    tranbrkp_gid int not null default 0,
    tran_mult tinyint not null default 0,
    excp_value double(15,2) not null default 0,
    primary key(gid),
    key idx_tran_gid(tran_gid),
    key idx_tranbrkp_gid(tran_gid,tranbrkp_gid),
    key idx_sno(sno)
  );

  if v_recontype_code = 'W' or v_recontype_code = 'B' then
    set v_sql = 'insert into recon_tmp_tvaluematched (tran_value,rec_count) ';
    set v_sql = concat(v_sql,'select excp_value,count(*) from recon_tmp_ttranvalue ');
    set v_sql = concat(v_sql,'where true ');
    set v_sql = concat(v_sql,in_condition,' ');
    set v_sql = concat(v_sql,'group by excp_value ');
    set v_sql = concat(v_sql,'having count(*) > 1 ');

    set v_sql = concat(v_sql,'and sum(if(tran_acc_mode=''C'',1,0)) > 0 ');
    set v_sql = concat(v_sql,'and sum(if(tran_acc_mode=''D'',1,0)) > 0 ');

    set v_sql = concat(v_sql,'order by excp_value desc ');
  else
    update recon_tmp_ttranvalue as a
    inner join recon_mst_trecondataset as b
      on a.recon_code = b.recon_code
      and a.dataset_code = b.dataset_code
      and b.active_status = 'Y'
      and b.delete_flag = 'N'
    set a.dataset_type = b.dataset_type;

    set v_sql = 'insert into recon_tmp_tvaluematched (tran_value,rec_count) ';
    set v_sql = concat(v_sql,'select a.excp_value,count(*) from recon_tmp_ttranvalue ');
    set v_sql = concat(v_sql,'where true ');
    set v_sql = concat(v_sql,in_condition,' ');
    set v_sql = concat(v_sql,'group by excp_value ');
    set v_sql = concat(v_sql,'having count(*) > 1 ');

    set v_sql = concat(v_sql,'and sum(if(dataset_type=''S'',1,0)) > 0 ');
    set v_sql = concat(v_sql,'and sum(if(dataset_type=''C'',1,0)) > 0 ');

    set v_sql = concat(v_sql,'order by excp_value desc ');
  end if;

  call pr_run_sql(v_sql,v_out_msg,v_out_result);

  if v_recontype_code = 'W' or v_recontype_code = 'B' then
    insert into recon_tmp_ttranvaluematched (sno,tran_gid,tranbrkp_gid,tran_mult,excp_value)
    select m.sno,t.tran_gid,t.tranbrkp_gid,-1,t.excp_value from recon_tmp_tvaluematched as m
    inner join recon_tmp_ttranvalue as t on m.tran_value = t.excp_value
                                  and t.tran_acc_mode = 'D'
                                  and t.delete_flag = 'N';

    insert into recon_tmp_ttranvaluematched (sno,tran_gid,tranbrkp_gid,tran_mult,excp_value)
    select m.sno,t.tran_gid,t.tranbrkp_gid,1,t.excp_value from recon_tmp_tvaluematched as m
    inner join recon_tmp_ttranvalue as t on m.tran_value = t.excp_value
                                  and t.tran_acc_mode = 'C'
                                  and t.delete_flag = 'N';
  else
    insert into recon_tmp_ttranvaluematched (sno,tran_gid,tranbrkp_gid,tran_mult,excp_value)
    select m.sno,t.tran_gid,t.tranbrkp_gid,t.tran_mult,t.excp_value from recon_tmp_tvaluematched as m
    inner join recon_tmp_ttranvalue as t on m.tran_value = t.excp_value
                                  and t.dataset_type = 'B'
                                  and t.delete_flag = 'N';

    insert into recon_tmp_ttranvaluematched (sno,tran_gid,tranbrkp_gid,tran_mult,excp_value)
    select m.sno,t.tran_gid,t.tranbrkp_gid,t.tran_mult,t.excp_value from recon_tmp_tvaluematched as m
    inner join recon_tmp_ttranvalue as t on m.tran_value = t.excp_value
                                  and t.dataset_type = 'T'
                                  and t.delete_flag = 'N';
  end if;

  if in_job_gid > 0 then
    call pr_upd_job(in_job_gid,'P','Moving record(s) from temporary table to preview table...',@msg,@result);
  end if;

  insert into recon_trn_tpreview
  (
    preview_gid,
    job_gid,
    rptsession_gid,
    preview_date,
    preview_value,
    rec_count,
    recon_code,
    insert_date,
    insert_by
  )
  select
    sno,
    in_job_gid,
    in_rptsession_gid,
    curdate(),
    tran_value,
    rec_count,
    in_recon_code,
    sysdate(),
    v_user_code
  from recon_tmp_tvaluematched;

  insert into recon_trn_tpreviewdtl
  (
    preview_gid,
    previewdtl_gid,
    job_gid,
    rptsession_gid,
    tran_gid,
    tranbrkp_gid,
    tran_mult,
    excp_value
  )
  select
    sno,
    gid,
    in_job_gid,
    in_rptsession_gid,
    tran_gid,
    tranbrkp_gid,
    tran_mult,
    excp_value
  from recon_tmp_ttranvaluematched;

  if in_job_gid > 0 then
    call pr_upd_job(in_job_gid,'P','Moving record(s) from preview table to report table...',@msg,@result);
  end if;

  call pr_run_previewreport(in_recon_code,in_job_gid,in_rptsession_gid,in_user_code,@msg,@result);

  drop temporary table if exists recon_tmp_trecondataset;
  drop temporary table if exists recon_tmp_ttranvalue;
  drop temporary table if exists recon_tmp_tvaluematched;
  drop temporary table if exists recon_tmp_ttranvaluematched;

  set out_msg = 'Success';
  set out_result = 1;

  if in_job_gid > 0 then
    call pr_upd_job(in_job_gid,'C','Completed',@msg,@result);
  end if;
end $$

DELIMITER ;