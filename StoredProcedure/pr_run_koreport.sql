﻿DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_koreport` $$
CREATE PROCEDURE `pr_run_koreport`(
  in in_job_gid int,
  in in_rptsession_gid int,
  in in_condition text,
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_sql text default '';

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  set in_job_gid = ifnull(in_job_gid,0);
  set in_rptsession_gid = ifnull(in_rptsession_gid,0);
  set in_user_code = ifnull(in_user_code,'');

  set v_sql = concat(v_sql,"insert into recon_rpt_tko
		select
		  ",cast(in_rptsession_gid as nchar)," as rptsession_gid,
		  ",cast(in_job_gid as nchar)," as job_gid,
		  b.kodtl_gid,
      '", in_user_code ,"' as user_code,
		  a.ko_gid,
      a.insert_date as ko_date,
      a.insert_by as ko_by,
      a.update_date as ko_del_date,
      a.update_by as ko_del_by,
		  a.ko_value as ko_gross_value,
		  b.tran_gid,
      null as base_value,
      '' as base_acc_mode,
      null as base_excp_value,
		  b.tranbrkp_gid,
		  a.recon_code,
		  d.recon_name,
		  a.rule_code,
		  e.rule_name,
		  a.reversal_flag,
		  a.manual_matchoff,
		  a.ko_reason,
		  a.ko_remark,
		  b.ko_value,
		  if(b.tranbrkp_gid > 0,'S','T') as rec_type,
		  c.dataset_code,
      f.dataset_name,
		  c.tran_date,
		  c.tran_acc_mode,
		  c.tran_value,
		  c.excp_value,
		  if(c.tran_mult = -1,b.ko_value,0),
		  if(c.tran_mult = 1,b.ko_value,0),
			c.col1,
			c.col2,
			c.col3,
			c.col4,
			c.col5,
			c.col6,
			c.col7,
			c.col8,
			c.col9,
			c.col10,
			c.col11,
			c.col12,
			c.col13,
			c.col14,
			c.col15,
			c.col16,
			c.col17,
			c.col18,
			c.col19,
			c.col20,
			c.col21,
			c.col22,
			c.col23,
			c.col24,
			c.col25,
			c.col26,
			c.col27,
			c.col28,
			c.col29,
			c.col30,
			c.col31,
			c.col32,
			c.col33,
			c.col34,
			c.col35,
			c.col36,
			c.col37,
			c.col38,
			c.col39,
			c.col40,
			c.col41,
			c.col42,
			c.col43,
			c.col44,
			c.col45,
			c.col46,
			c.col47,
			c.col48,
			c.col49,
			c.col50,
			c.col51,
			c.col52,
			c.col53,
			c.col54,
			c.col55,
			c.col56,
			c.col57,
			c.col58,
			c.col59,
			c.col60,
			c.col61,
			c.col62,
			c.col63,
			c.col64,
			c.col65,
			c.col66,
			c.col67,
			c.col68,
			c.col69,
			c.col70,
			c.col71,
			c.col72,
			c.col73,
			c.col74,
			c.col75,
			c.col76,
			c.col77,
			c.col78,
			c.col79,
			c.col80,
			c.col81,
			c.col82,
			c.col83,
			c.col84,
			c.col85,
			c.col86,
			c.col87,
			c.col88,
			c.col89,
			c.col90,
			c.col91,
			c.col92,
			c.col93,
			c.col94,
			c.col95,
			c.col96,
			c.col97,
			c.col98,
			c.col99,
			c.col100,
			c.col101,
			c.col102,
			c.col103,
			c.col104,
			c.col105,
			c.col106,
			c.col107,
			c.col108,
			c.col109,
			c.col110,
			c.col111,
			c.col112,
			c.col113,
			c.col114,
			c.col115,
			c.col116,
			c.col117,
			c.col118,
			c.col119,
			c.col120,
			c.col121,
			c.col122,
			c.col123,
			c.col124,
			c.col125,
			c.col126,
			c.col127,
			c.col128,
		  c.tran_remark1,
		  c.tran_remark2,
      '' as tranbrkptype_name
		from recon_trn_tko as a
		inner join recon_trn_tkodtl as b on a.ko_gid = b.ko_gid and b.tranbrkp_gid = 0 and b.delete_flag = 'N'
		inner join recon_trn_ttranko as c on b.tran_gid = c.tran_gid and c.delete_flag = 'N'
		inner join recon_mst_trecon as d on a.recon_code = d.recon_code and d.delete_flag = 'N'
		left join recon_mst_trule as e on a.rule_code = e.rule_code and e.delete_flag = 'N'
    left join recon_mst_tdataset as f on c.dataset_code = f.dataset_code and f.delete_flag = 'N'
		where true ", in_condition,"

    union

		select
		  ",cast(in_rptsession_gid as nchar)," as rptsession_gid,
		  ",cast(in_job_gid as nchar)," as job_gid,
		  b.kodtl_gid,
      '", in_user_code ,"' as user_code,
		  a.ko_gid,
      a.insert_date as ko_date,
      a.insert_by as ko_by,
      a.update_date as ko_del_date,
      a.update_by as ko_del_by,
		  a.ko_value as ko_gross_value,
		  b.tran_gid,
      null as base_value,
      '' as base_acc_mode,
      null as base_excp_value,
		  b.tranbrkp_gid,
		  a.recon_code,
		  d.recon_name,
		  a.rule_code,
		  e.rule_name,
		  a.reversal_flag,
		  a.manual_matchoff,
		  a.ko_reason,
		  a.ko_remark,
		  b.ko_value,
		  if(b.tranbrkp_gid > 0,'S','T') as rec_type,
		  c.dataset_code,
      f.dataset_name,
		  c.tran_date,
		  c.tran_acc_mode,
		  c.tran_value,
		  c.excp_value,
		  if(c.tran_mult = -1,b.ko_value,0),
		  if(c.tran_mult = 1,b.ko_value,0),
			c.col1,
			c.col2,
			c.col3,
			c.col4,
			c.col5,
			c.col6,
			c.col7,
			c.col8,
			c.col9,
			c.col10,
			c.col11,
			c.col12,
			c.col13,
			c.col14,
			c.col15,
			c.col16,
			c.col17,
			c.col18,
			c.col19,
			c.col20,
			c.col21,
			c.col22,
			c.col23,
			c.col24,
			c.col25,
			c.col26,
			c.col27,
			c.col28,
			c.col29,
			c.col30,
			c.col31,
			c.col32,
			c.col33,
			c.col34,
			c.col35,
			c.col36,
			c.col37,
			c.col38,
			c.col39,
			c.col40,
			c.col41,
			c.col42,
			c.col43,
			c.col44,
			c.col45,
			c.col46,
			c.col47,
			c.col48,
			c.col49,
			c.col50,
			c.col51,
			c.col52,
			c.col53,
			c.col54,
			c.col55,
			c.col56,
			c.col57,
			c.col58,
			c.col59,
			c.col60,
			c.col61,
			c.col62,
			c.col63,
			c.col64,
			c.col65,
			c.col66,
			c.col67,
			c.col68,
			c.col69,
			c.col70,
			c.col71,
			c.col72,
			c.col73,
			c.col74,
			c.col75,
			c.col76,
			c.col77,
			c.col78,
			c.col79,
			c.col80,
			c.col81,
			c.col82,
			c.col83,
			c.col84,
			c.col85,
			c.col86,
			c.col87,
			c.col88,
			c.col89,
			c.col90,
			c.col91,
			c.col92,
			c.col93,
			c.col94,
			c.col95,
			c.col96,
			c.col97,
			c.col98,
			c.col99,
			c.col100,
			c.col101,
			c.col102,
			c.col103,
			c.col104,
			c.col105,
			c.col106,
			c.col107,
			c.col108,
			c.col109,
			c.col110,
			c.col111,
			c.col112,
			c.col113,
			c.col114,
			c.col115,
			c.col116,
			c.col117,
			c.col118,
			c.col119,
			c.col120,
			c.col121,
			c.col122,
			c.col123,
			c.col124,
			c.col125,
			c.col126,
			c.col127,
			c.col128,
		  c.tran_remark1,
		  c.tran_remark2,
      '' as tranbrkptype_name
		from recon_trn_tko as a
		inner join recon_trn_tkodtl as b on a.ko_gid = b.ko_gid and b.tranbrkp_gid = 0 and b.delete_flag = 'N'
		inner join recon_trn_ttran as c on b.tran_gid = c.tran_gid and c.delete_flag = 'N'
		inner join recon_mst_trecon as d on a.recon_code = d.recon_code and d.delete_flag = 'N'
		left join recon_mst_trule as e on a.rule_code = e.rule_code and e.delete_flag = 'N'
    left join recon_mst_tdataset as f on c.dataset_code = f.dataset_code and f.delete_flag = 'N'
		where true ", in_condition,"

    union

		select
		  ",cast(in_rptsession_gid as nchar)," as rptsession_gid,
		  ",cast(in_job_gid as nchar)," as job_gid,
		  b.kodtl_gid,
      '", in_user_code ,"' as user_code,
		  a.ko_gid,
      a.insert_date as ko_date,
      a.insert_by as ko_by,
      a.update_date as ko_del_date,
      a.update_by as ko_del_by,
		  a.ko_value as ko_gross_value,
		  b.tran_gid,
      ifnull(g.tran_value,h.tran_value) as base_value,
      ifnull(g.tran_acc_mode,h.tran_acc_mode) as base_acc_mode,
      ifnull(g.excp_value,h.excp_value) as base_excp_value,
		  b.tranbrkp_gid,
		  a.recon_code,
		  d.recon_name,
		  a.rule_code,
		  e.rule_name,
		  a.reversal_flag,
		  a.manual_matchoff,
		  a.ko_reason,
		  a.ko_remark,
		  b.ko_value,
		  if(b.tranbrkp_gid > 0,'S','T') as rec_type,
		  c.tranbrkp_dataset_code,
      f.dataset_name,
		  c.tran_date,
	    c.tran_acc_mode,
		  c.tran_value,
		  c.excp_value,
		  if(c.tran_mult = -1,b.ko_value,0),
		  if(c.tran_mult = 1,b.ko_value,0),
			c.col1,
			c.col2,
			c.col3,
			c.col4,
			c.col5,
			c.col6,
			c.col7,
			c.col8,
			c.col9,
			c.col10,
			c.col11,
			c.col12,
			c.col13,
			c.col14,
			c.col15,
			c.col16,
			c.col17,
			c.col18,
			c.col19,
			c.col20,
			c.col21,
			c.col22,
			c.col23,
			c.col24,
			c.col25,
			c.col26,
			c.col27,
			c.col28,
			c.col29,
			c.col30,
			c.col31,
			c.col32,
			c.col33,
			c.col34,
			c.col35,
			c.col36,
			c.col37,
			c.col38,
			c.col39,
			c.col40,
			c.col41,
			c.col42,
			c.col43,
			c.col44,
			c.col45,
			c.col46,
			c.col47,
			c.col48,
			c.col49,
			c.col50,
			c.col51,
			c.col52,
			c.col53,
			c.col54,
			c.col55,
			c.col56,
			c.col57,
			c.col58,
			c.col59,
			c.col60,
			c.col61,
			c.col62,
			c.col63,
			c.col64,
			c.col65,
			c.col66,
			c.col67,
			c.col68,
			c.col69,
			c.col70,
			c.col71,
			c.col72,
			c.col73,
			c.col74,
			c.col75,
			c.col76,
			c.col77,
			c.col78,
			c.col79,
			c.col80,
			c.col81,
			c.col82,
			c.col83,
			c.col84,
			c.col85,
			c.col86,
			c.col87,
			c.col88,
			c.col89,
			c.col90,
			c.col91,
			c.col92,
			c.col93,
			c.col94,
			c.col95,
			c.col96,
			c.col97,
			c.col98,
			c.col99,
			c.col100,
			c.col101,
			c.col102,
			c.col103,
			c.col104,
			c.col105,
			c.col106,
			c.col107,
			c.col108,
			c.col109,
			c.col110,
			c.col111,
			c.col112,
			c.col113,
			c.col114,
			c.col115,
			c.col116,
			c.col117,
			c.col118,
			c.col119,
			c.col120,
			c.col121,
			c.col122,
			c.col123,
			c.col124,
			c.col125,
			c.col126,
			c.col127,
			c.col128,
		  '' as tran_remark1,
		  '' as tran_remark2,
      f.dataset_name
		from recon_trn_tko as a
		inner join recon_trn_tkodtl as b on a.ko_gid = b.ko_gid
		inner join recon_trn_ttranbrkpko as c on b.tranbrkp_gid = c.tranbrkp_gid and c.delete_flag = 'N'
		inner join recon_mst_trecon as d on a.recon_code = d.recon_code and d.delete_flag = 'N'
		left join recon_mst_trule as e on a.rule_code = e.rule_code and e.delete_flag = 'N'
    left join recon_mst_tdataset as f on c.tranbrkp_dataset_code = f.dataset_code and f.delete_flag = 'N'
    left join recon_trn_ttranko as g on c.tran_gid = g.tran_gid and g.delete_flag = 'N'
    left join recon_trn_ttran as h on c.tran_gid = h.tran_gid and h.delete_flag = 'N'
		where true ", in_condition,"

    union

		select
		  ",cast(in_rptsession_gid as nchar)," as rptsession_gid,
		  ",cast(in_job_gid as nchar)," as job_gid,
		  b.kodtl_gid,
      '", in_user_code ,"' as user_code,
		  a.ko_gid,
      a.insert_date as ko_date,
      a.insert_by as ko_by,
      a.update_date as ko_del_date,
      a.update_by as ko_del_by,
		  a.ko_value as ko_gross_value,
		  b.tran_gid,
      ifnull(g.tran_value,h.tran_value) as base_value,
      ifnull(g.tran_acc_mode,h.tran_acc_mode) as base_acc_mode,
      ifnull(g.excp_value,h.excp_value) as base_excp_value,
		  b.tranbrkp_gid,
		  a.recon_code,
		  d.recon_name,
		  a.rule_code,
		  e.rule_name,
		  a.reversal_flag,
		  a.manual_matchoff,
		  a.ko_reason,
		  a.ko_remark,
		  b.ko_value,
		  if(b.tranbrkp_gid > 0,'S','T') as rec_type,
		  c.tranbrkp_dataset_code,
      f.dataset_name,
		  c.tran_date,
		  c.tran_acc_mode,
		  c.tran_value,
		  c.excp_value,
		  if(c.tran_mult = -1,b.ko_value,0),
		  if(c.tran_mult = 1,b.ko_value,0),
			c.col1,
			c.col2,
			c.col3,
			c.col4,
			c.col5,
			c.col6,
			c.col7,
			c.col8,
			c.col9,
			c.col10,
			c.col11,
			c.col12,
			c.col13,
			c.col14,
			c.col15,
			c.col16,
			c.col17,
			c.col18,
			c.col19,
			c.col20,
			c.col21,
			c.col22,
			c.col23,
			c.col24,
			c.col25,
			c.col26,
			c.col27,
			c.col28,
			c.col29,
			c.col30,
			c.col31,
			c.col32,
			c.col33,
			c.col34,
			c.col35,
			c.col36,
			c.col37,
			c.col38,
			c.col39,
			c.col40,
			c.col41,
			c.col42,
			c.col43,
			c.col44,
			c.col45,
			c.col46,
			c.col47,
			c.col48,
			c.col49,
			c.col50,
			c.col51,
			c.col52,
			c.col53,
			c.col54,
			c.col55,
			c.col56,
			c.col57,
			c.col58,
			c.col59,
			c.col60,
			c.col61,
			c.col62,
			c.col63,
			c.col64,
			c.col65,
			c.col66,
			c.col67,
			c.col68,
			c.col69,
			c.col70,
			c.col71,
			c.col72,
			c.col73,
			c.col74,
			c.col75,
			c.col76,
			c.col77,
			c.col78,
			c.col79,
			c.col80,
			c.col81,
			c.col82,
			c.col83,
			c.col84,
			c.col85,
			c.col86,
			c.col87,
			c.col88,
			c.col89,
			c.col90,
			c.col91,
			c.col92,
			c.col93,
			c.col94,
			c.col95,
			c.col96,
			c.col97,
			c.col98,
			c.col99,
			c.col100,
			c.col101,
			c.col102,
			c.col103,
			c.col104,
			c.col105,
			c.col106,
			c.col107,
			c.col108,
			c.col109,
			c.col110,
			c.col111,
			c.col112,
			c.col113,
			c.col114,
			c.col115,
			c.col116,
			c.col117,
			c.col118,
			c.col119,
			c.col120,
			c.col121,
			c.col122,
			c.col123,
			c.col124,
			c.col125,
			c.col126,
			c.col127,
			c.col128,
		  '' as tran_remark1,
		  '' as tran_remark2,
      f.dataset_name
		from recon_trn_tko as a
		inner join recon_trn_tkodtl as b on a.ko_gid = b.ko_gid 
		inner join recon_trn_ttranbrkp as c on b.tranbrkp_gid = c.tranbrkp_gid and c.delete_flag = 'N'
		inner join recon_mst_trecon as d on a.recon_code = d.recon_code and d.delete_flag = 'N'
		left join recon_mst_trule as e on a.rule_code = e.rule_code and e.delete_flag = 'N'
    left join recon_mst_tdataset as f on c.tranbrkp_dataset_code = f.dataset_code and f.delete_flag = 'N'
    left join recon_trn_ttranko as g on c.tran_gid = g.tran_gid and g.delete_flag = 'N'
    left join recon_trn_ttran as h on c.tran_gid = h.tran_gid and h.delete_flag = 'N'
		where true ", in_condition,"
  ");

  call pr_run_sql(v_sql,@msg,@result);

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;