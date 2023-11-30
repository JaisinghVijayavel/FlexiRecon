DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_manualmatchreport` $$
CREATE PROCEDURE `pr_run_manualmatchreport`(
  in in_job_gid int,
  in in_rptsession_gid int,
  in in_condition text,
  in in_user_code varchar(16),
  out out_msg text,
  out out_result int
)
me:BEGIN


  declare v_sql text default '';

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  drop temporary table if exists recon_tmp_tmanualmatch;
  drop temporary table if exists recon_tmp_ttran;
  drop temporary table if exists recon_tmp_ttrangid;

  create temporary table recon_tmp_tmanualmatch select * from recon_trn_tmanualtran where 1 = 2;
  alter table recon_tmp_tmanualmatch add primary key(match_gid,scheduler_gid,tran_gid);
  create index idx_scheduler_gid on recon_tmp_tmanualmatch(scheduler_gid);
  create index idx_tran_gid on recon_tmp_tmanualmatch(scheduler_gid,tran_gid);
  create index idx_match_gid on recon_tmp_tmanualmatch(scheduler_gid,match_gid);

  alter table recon_tmp_tmanualmatch add file_name varchar(128);
  alter table recon_tmp_tmanualmatch add scheduled_date datetime;
  alter table recon_tmp_tmanualmatch add scheduler_parameters text;
  alter table recon_tmp_tmanualmatch add scheduler_initiated_by varchar(32);
  alter table recon_tmp_tmanualmatch add recon_name varchar(128);

  create temporary table recon_tmp_ttran select * from recon_trn_ttran where 1 = 2;
  alter table recon_tmp_ttran add primary key(tran_gid);

  create temporary table recon_tmp_ttrangid(
    tran_gid int unsigned not null,
    primary key (tran_gid)
  );

  set v_sql = concat("insert into recon_tmp_tmanualmatch (
      match_gid,scheduler_gid,tran_gid,rec_slno,recon_code,dataset_code,ko_value,ko_acc_mode,ko_reason,ko_status,
      file_name,scheduled_date,scheduler_parameters,scheduler_initiated_by)
    select
      a.match_gid,a.scheduler_gid,a.tran_gid,a.rec_slno,a.recon_code,a.dataset_code,
      a.ko_value,a.ko_acc_mode,a.ko_reason,a.ko_status,
      b.file_name,b.scheduled_date,b.scheduler_parameters,b.scheduler_initiated_by
    from recon_trn_tmanualtran as a
    inner join con_trn_tscheduler as b on a.scheduler_gid = b.scheduler_gid and b.delete_flag = 'N'
    where a.delete_flag = 'N' ",in_condition,"

    union

    select
      a.match_gid,a.scheduler_gid,a.tran_gid,a.rec_slno,a.recon_code,a.dataset_code,
      a.ko_value,a.ko_acc_mode,a.ko_reason,a.ko_status,
      b.file_name,b.scheduled_date,b.scheduler_parameters,b.scheduler_initiated_by
    from recon_trn_tmanualtranmatch as a
    inner join con_trn_tscheduler as b on a.scheduler_gid = b.scheduler_gid and b.delete_flag = 'N'
    where a.delete_flag = 'N' ",in_condition,"
   ");

  call pr_run_sql(v_sql,@msg,@result);

  update recon_tmp_tmanualmatch as a
  inner join recon_mst_trecon as b on a.recon_code = b.recon_code
  set a.recon_name = b.recon_name;


  set v_sql = "insert into recon_tmp_ttrangid select distinct tran_gid from recon_tmp_tmanualmatch";

  call pr_run_sql(v_sql,@msg,@result);

  
  set v_sql = concat("insert into recon_tmp_ttran
    select * from recon_trn_ttran where tran_gid in (select tran_gid from recon_tmp_ttrangid)");
  call pr_run_sql(v_sql,@msg,@result);

  set v_sql = concat("insert into recon_tmp_ttran
    select * from recon_trn_ttranko where tran_gid in (select tran_gid from recon_tmp_ttrangid)");
  call pr_run_sql(v_sql,@msg,@result);

  set @rec_slno := 0;

  set v_sql = concat("insert into recon_rpt_tmanualmatch (
			rptsession_gid,
			job_gid,
			rec_slno,
			user_code,
			recon_code,
			recon_name,
			match_gid,
			ko_acc_mode,
			ko_value,
			ko_reason,
      ko_status,
			scheduler_gid,
			scheduled_date,
			scheduler_initiated_by,
			file_name,
			tran_gid,
			dataset_code,
			tran_date,
			tran_value,
			tran_mult,
			tran_acc_mode,
			excp_value,
			mapped_value,
			col1,
			col2,
			col3,
			col4,
			col5,
			col6,
			col7,
			col8,
			col9,
			col10,
			col11,
			col12,
			col13,
			col14,
			col15,
			col16,
			col17,
			col18,
			col19,
			col20,
			col21,
			col22,
			col23,
			col24,
			col25,
			col26,
			col27,
			col28,
			col29,
			col30,
			col31,
			col32,
			col33,
			col34,
			col35,
			col36,
			col37,
			col38,
			col39,
			col40,
			col41,
			col42,
			col43,
			col44,
			col45,
			col46,
			col47,
			col48,
			col49,
			col50,
			col51,
			col52,
			col53,
			col54,
			col55,
			col56,
			col57,
			col58,
			col59,
			col60,
			col61,
			col62,
			col63,
			col64,
			col65,
			col66,
			col67,
			col68,
			col69,
			col70,
			col71,
			col72,
			col73,
			col74,
			col75,
			col76,
			col77,
			col78,
			col79,
			col80,
			col81,
			col82,
			col83,
			col84,
			col85,
			col86,
			col87,
			col88,
			col89,
			col90,
			col91,
			col92,
			col93,
			col94,
			col95,
			col96,
			col97,
			col98,
			col99,
			col100,
			col101,
			col102,
			col103,
			col104,
			col105,
			col106,
			col107,
			col108,
			col109,
			col110,
			col111,
			col112,
			col113,
			col114,
			col115,
			col116,
			col117,
			col118,
			col119,
			col120,
			col121,
			col122,
			col123,
			col124,
			col125,
			col126,
			col127,
			col128,
			tran_remark1,
			tran_remark2,
			ko_gid,
			ko_date
    )
		select
      ",cast(in_rptsession_gid as nchar),",
		  ",cast(in_job_gid as nchar)," as job_gid,
		  @rec_slno:=@rec_slno+1,
      '", in_user_code ,"',
			a.recon_code,
			a.recon_name,
			a.match_gid,
			a.ko_acc_mode,
			a.ko_value,
			a.ko_reason,
      a.ko_status,
			a.scheduler_gid,
			a.scheduled_date,
			a.scheduler_initiated_by,
			a.file_name,
			b.tran_gid,
			b.dataset_code,
			b.tran_date,
			b.tran_value,
			b.tran_mult,
			b.tran_acc_mode,
			b.excp_value,
			b.mapped_value,
			b.col1,
			b.col2,
			b.col3,
			b.col4,
			b.col5,
			b.col6,
			b.col7,
			b.col8,
			b.col9,
			b.col10,
			b.col11,
			b.col12,
			b.col13,
			b.col14,
			b.col15,
			b.col16,
			b.col17,
			b.col18,
			b.col19,
			b.col20,
			b.col21,
			b.col22,
			b.col23,
			b.col24,
			b.col25,
			b.col26,
			b.col27,
			b.col28,
			b.col29,
			b.col30,
			b.col31,
			b.col32,
			b.col33,
			b.col34,
			b.col35,
			b.col36,
			b.col37,
			b.col38,
			b.col39,
			b.col40,
			b.col41,
			b.col42,
			b.col43,
			b.col44,
			b.col45,
			b.col46,
			b.col47,
			b.col48,
			b.col49,
			b.col50,
			b.col51,
			b.col52,
			b.col53,
			b.col54,
			b.col55,
			b.col56,
			b.col57,
			b.col58,
			b.col59,
			b.col60,
			b.col61,
			b.col62,
			b.col63,
			b.col64,
			b.col65,
			b.col66,
			b.col67,
			b.col68,
			b.col69,
			b.col70,
			b.col71,
			b.col72,
			b.col73,
			b.col74,
			b.col75,
			b.col76,
			b.col77,
			b.col78,
			b.col79,
			b.col80,
			b.col81,
			b.col82,
			b.col83,
			b.col84,
			b.col85,
			b.col86,
			b.col87,
			b.col88,
			b.col89,
			b.col90,
			b.col91,
			b.col92,
			b.col93,
			b.col94,
			b.col95,
			b.col96,
			b.col97,
			b.col98,
			b.col99,
			b.col100,
			b.col101,
			b.col102,
			b.col103,
			b.col104,
			b.col105,
			b.col106,
			b.col107,
			b.col108,
			b.col109,
			b.col110,
			b.col111,
			b.col112,
			b.col113,
			b.col114,
			b.col115,
			b.col116,
			b.col117,
			b.col118,
			b.col119,
			b.col120,
			b.col121,
			b.col122,
			b.col123,
			b.col124,
			b.col125,
			b.col126,
			b.col127,
			b.col128,
			b.tran_remark1,
			b.tran_remark2,
			b.ko_gid,
			b.ko_date
		from recon_tmp_tmanualmatch as a
    inner join recon_tmp_ttran as b on a.tran_gid = b.tran_gid
		where a.delete_flag = 'N' ", in_condition,"
  ");

  call pr_run_sql(v_sql,@msg,@result);

  set out_msg = 'Success';
  set out_result = 1;

  
  drop temporary table if exists recon_tmp_tmanualmatch;
  drop temporary table if exists recon_tmp_ttran;
  drop temporary table if exists recon_tmp_ttrangid;
end $$

DELIMITER ;