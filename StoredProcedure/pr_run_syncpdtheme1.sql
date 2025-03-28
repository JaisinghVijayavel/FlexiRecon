﻿DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_syncpdtheme1` $$
CREATE PROCEDURE `pr_run_syncpdtheme1`
(
  in in_recon_code text,
  out out_msg text,
  out out_result int
)
me:BEGIN
  /*
    Created By : Vijayavel
    Created Date : 21-03-2025

    Updated By : Vijayavel
    updated Date : 

    Version : 1
  */

  declare v_sql text default '';

  declare v_iuttheme_field text default '';
  declare v_noniuttheme_field text default '';
  declare v_iutflag_field text default '';
  declare v_iutvalue_field text default '';
  declare v_closingbalance_field text default '';
  declare v_fromunit_field text default '';
  declare v_tounit_field text default '';
  declare v_iutrefno_field text default '';
  declare v_actiontaken_field text default '';
  declare v_entryflag_field text default '';
  declare v_entryrefno_field text default '';

  declare v_recon_iutflag_field text default '';
  declare v_recon_iutvalue_field text default '';
  declare v_recon_closingbalance_field text default '';
  declare v_recon_fromunit_field text default '';
  declare v_recon_tounit_field text default '';
  declare v_recon_iutrefno_field text default '';
  declare v_recon_actiontaken_field text default '';
  declare v_recon_entryflag_field text default '';
  declare v_recon_entryrefno_field text default '';
	declare v_recon_code_field text default '';

	declare v_pdrecon_code text default '';
  declare v_pdtran_table text default '';
  declare v_pdtranbrkp_table text default '';

  declare v_tran_table text default '';
  declare v_tranbrkp_table text default '';

  declare v_concurrent_ko_flag text default '';

  -- concurrent KO flag
  set v_concurrent_ko_flag = fn_get_configvalue('concurrent_ko_flag');

  if v_concurrent_ko_flag = 'Y' then
    set v_tran_table = concat(in_recon_code,'_tran');
    set v_tranbrkp_table = concat(in_recon_code,'_tranbrkp');
  else
    set v_tran_table = 'recon_trn_ttran';
    set v_tranbrkp_table = 'recon_trn_ttranbrkp';
  end if;

  select
    recon_field_name into v_iuttheme_field
  from recon_mst_treconfield
  where recon_code = in_recon_code
  and recon_field_desc = 'IUT Theme'
  and active_status = 'Y'
  and delete_flag = 'N';

  set v_iuttheme_field = fn_get_reconfieldfromdesc(in_recon_code,'IUT Theme');
  set v_noniuttheme_field = fn_get_reconfieldfromdesc(in_recon_code,'Non IUT Theme');
  set v_iutvalue_field = fn_get_reconfieldfromdesc(in_recon_code,'IUT Value');
  set v_iutflag_field = fn_get_reconfieldfromdesc(in_recon_code,'IUT IP/OP');
  set v_closingbalance_field = fn_get_reconfieldfromdesc(in_recon_code,'Closing Balance');
  set v_fromunit_field = fn_get_reconfieldfromdesc(in_recon_code,'From Unit');
  set v_tounit_field = fn_get_reconfieldfromdesc(in_recon_code,'To Unit');
  set v_iutrefno_field = fn_get_reconfieldfromdesc(in_recon_code,'Entry Ref No');
  set v_actiontaken_field = fn_get_reconfieldfromdesc(in_recon_code,'Action To Be Taken');
  set v_entryflag_field = fn_get_reconfieldfromdesc(in_recon_code,'Entry Pass Flag');
  set v_entryrefno_field = fn_get_reconfieldfromdesc(in_recon_code,'Entry Pass Ref No');

  set v_recon_iutflag_field = fn_get_reconfieldfromdesc('RE170','IUT Flag');
  set v_recon_iutvalue_field = fn_get_reconfieldfromdesc('RE170','Entry Value');
  set v_recon_closingbalance_field = fn_get_reconfieldfromdesc('RE170','Closing Balance');
  set v_recon_fromunit_field = fn_get_reconfieldfromdesc('RE170','From Unit');
  set v_recon_tounit_field = fn_get_reconfieldfromdesc('RE170','To Unit');
  set v_recon_iutrefno_field = fn_get_reconfieldfromdesc('RE170','IUT Ref No');
  set v_recon_actiontaken_field = fn_get_reconfieldfromdesc('RE170','Action To Be Taken');
  set v_recon_entryflag_field = fn_get_reconfieldfromdesc('RE170','Entry Pass Flag');
  set v_recon_entryrefno_field = fn_get_reconfieldfromdesc('RE170','Entry Ref No');

	set v_recon_code_field = 'col38';

  drop temporary table if exists recon_tmp_tiuttheme;

  create temporary table recon_tmp_tiuttheme(
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned not null default 0,
    iut_theme text,
    iut_flag text,
    noniut_theme text,
    iut_value text,
    closing_balance text,
    from_unit text,
    to_unit text,
    iut_ref_no text,
    entry_flag text,
    entry_ref_no text,
    action_tobe_taken text,
    PRIMARY KEY (tran_gid,tranbrkp_gid)
  ) ENGINE = MyISAM;

	-- pdrecon block
	pdrecon_block:begin
		declare pdrecon_done int default 0;

		declare pdrecon_cursor cursor for
			select pdrecon_code from recon_mst_tpdrecon
			where cluster_name = 'CHENNAI'
			and active_status = 'Y'
			and delete_flag = 'N';

		declare continue handler for not found set pdrecon_done=1;

		open pdrecon_cursor;

		pdrecon_loop: loop
			fetch pdrecon_cursor into v_pdrecon_code;
			if pdrecon_done = 1 then leave pdrecon_loop; end if;

			if v_concurrent_ko_flag = 'Y' then
				set v_pdtran_table = concat(v_pdrecon_code,'_tran');
				set v_pdtranbrkp_table = concat(v_pdrecon_code,'_tranbrkp');
			else
				set v_pdtran_table = 'recon_trn_ttran';
				set v_pdtranbrkp_table = 'recon_trn_ttranbrkp';
			end if;

			-- pd sync start

			-- clear theme & iut related fields in recon table
			set v_sql = concat("update ",v_pdtran_table," as b
				set
					b.theme_code = '',
					b.tran_remark2 = '',
					b.",v_recon_iutflag_field," = '',
					b.",v_recon_iutvalue_field," = '',
					b.",v_recon_closingbalance_field," = '',
					b.",v_recon_fromunit_field," = '',
					b.",v_recon_tounit_field," = '',
					b.",v_recon_actiontaken_field," = '',
					b.",v_recon_entryflag_field," = '',
					b.",v_recon_entryrefno_field," = '',
					b.",v_recon_iutrefno_field," = ''
				where b.recon_code = '",v_pdrecon_code,"'
				and b.",v_recon_iutflag_field," in ('IUT - IP','IUT - OP')
				and b.tran_remark2 like 'IUT%'
				and b.tran_remark2 not like '%manual%'
				and b.delete_flag = 'N'");

			call pr_run_sql2(v_sql,@msg,@result);

			set v_sql = concat("update ",v_pdtranbrkp_table," as b
				set
					b.theme_code = '',
					b.tran_remark2 = '',
					b.",v_recon_iutflag_field," = '',
					b.",v_recon_iutvalue_field," = '',
					b.",v_recon_closingbalance_field," = '',
					b.",v_recon_fromunit_field," = '',
					b.",v_recon_tounit_field," = '',
					b.",v_recon_actiontaken_field," = '',
					b.",v_recon_entryflag_field," = '',
					b.",v_recon_entryrefno_field," = '',
					b.",v_recon_iutrefno_field," = ''
				where b.recon_code = '",v_pdrecon_code,"'
				and b.",v_recon_iutflag_field," in ('IUT - IP','IUT - OP')
				and b.tran_remark2 like 'IUT%'
				and b.tran_remark2 not like '%manual%'
				and b.delete_flag = 'N'");

			call pr_run_sql2(v_sql,@msg,@result);

			-- clear IUT related fields
			set v_sql = concat("update ",v_pdtran_table," as b
				set
					b.",v_recon_iutflag_field," = '',
					b.",v_recon_iutvalue_field," = '',
					b.",v_recon_closingbalance_field," = '',
					b.",v_recon_fromunit_field," = '',
					b.",v_recon_tounit_field," = '',
					b.",v_recon_actiontaken_field," = '',
					b.",v_recon_entryflag_field," = '',
					b.",v_recon_entryrefno_field," = '',
					b.",v_recon_iutrefno_field," = ''
				where b.recon_code = '",v_pdrecon_code,"'
				and b.",v_recon_iutvalue_field," <> ''
				and b.delete_flag = 'N'");

			call pr_run_sql2(v_sql,@msg,@result);

			set v_sql = concat("update ",v_pdtranbrkp_table," as b
				set
					b.",v_recon_iutflag_field," = '',
					b.",v_recon_iutvalue_field," = '',
					b.",v_recon_closingbalance_field," = '',
					b.",v_recon_fromunit_field," = '',
					b.",v_recon_tounit_field," = '',
					b.",v_recon_actiontaken_field," = '',
					b.",v_recon_entryflag_field," = '',
					b.",v_recon_entryrefno_field," = '',
					b.",v_recon_iutrefno_field," = ''
				where b.recon_code = '",v_pdrecon_code,"'
				and b.",v_recon_iutvalue_field," <> ''
				and b.delete_flag = 'N'");

			call pr_run_sql2(v_sql,@msg,@result);

			-- move iut theme in temp table
			set v_sql = concat("insert into recon_tmp_tiuttheme
				(
					tran_gid,tranbrkp_gid,iut_theme,iut_flag,iut_value,closing_balance,
					from_unit,to_unit,iut_ref_no,entry_flag,entry_ref_no,action_tobe_taken
				)
				select z.* from (
				select
					cast(col1 as signed),
					cast(col2 as signed),
					",v_iuttheme_field,",
					",v_iutflag_field,",
					",v_iutvalue_field,",
					",v_closingbalance_field,",
					",v_fromunit_field,",
					",v_tounit_field,",
					",v_iutrefno_field,",
					",v_entryflag_field,",
					",v_entryrefno_field,",
					",v_actiontaken_field,"
				from ",v_tran_table,"
				where recon_code = '",in_recon_code,"'
				and ",v_recon_code_field," = '",v_pdrecon_code,"'
				and ",v_iuttheme_field," <> ''
				and delete_flag = 'N'  LOCK IN SHARE MODE) as z
				");

			call pr_run_sql2(v_sql,@msg,@result);

			-- move noniut theme in temp table
			set v_sql = concat("insert ignore into recon_tmp_tiuttheme
				(
					tran_gid,tranbrkp_gid,noniut_theme,iut_value,closing_balance,
					from_unit,to_unit,iut_ref_no,entry_flag,entry_ref_no,action_tobe_taken
				)
				select
					cast(col1 as signed),
					cast(col2 as signed),
					",v_noniuttheme_field,",
					",v_iutvalue_field,",
					",v_closingbalance_field,",
					",v_fromunit_field,",
					",v_tounit_field,",
					",v_iutrefno_field,",
					",v_entryflag_field,",
					",v_entryrefno_field,",
					",v_actiontaken_field,"
				from ",v_tran_table,"
				where recon_code = '",in_recon_code,"'
				and ",v_recon_code_field," = '",v_pdrecon_code,"'
				and ",v_noniuttheme_field," <> ''
				and delete_flag = 'N'
				");

			call pr_run_sql2(v_sql,@msg,@result);

			-- sync iut theme in pd recon
			set v_sql = concat("update ",v_pdtran_table," as a
				inner join recon_tmp_tiuttheme as b on a.tran_gid = b.tran_gid
					and b.tranbrkp_gid = 0
					and b.iut_theme <> ''
				set
					a.theme_code = b.iut_theme,
					a.tran_remark2 = b.iut_theme,
					a.",v_recon_iutflag_field," = b.iut_flag,
					a.",v_recon_iutvalue_field," = b.iut_value,
					a.",v_recon_closingbalance_field," = b.closing_balance,
					a.",v_recon_fromunit_field," = b.from_unit,
					a.",v_recon_tounit_field," = b.to_unit,
					a.",v_recon_actiontaken_field," = b.action_tobe_taken,
					a.",v_recon_entryflag_field," = b.entry_flag,
					a.",v_recon_entryrefno_field," = b.entry_ref_no,
					a.",v_recon_iutrefno_field," = b.iut_ref_no
				");

			call pr_run_sql2(v_sql,@msg,@result);

			-- sync iut theme in pd recon
			set v_sql = concat("update ",v_pdtranbrkp_table," as a
				inner join recon_tmp_tiuttheme as b on a.tran_gid = b.tran_gid
					and b.tranbrkp_gid = a.tranbrkp_gid
					and b.iut_theme <> ''
				set
					a.theme_code = b.iut_theme,
					a.tran_remark2 = b.iut_theme,
					a.",v_recon_iutflag_field," = b.iut_flag,
					a.",v_recon_iutvalue_field," = b.iut_value,
					a.",v_recon_closingbalance_field," = b.closing_balance,
					a.",v_recon_fromunit_field," = b.from_unit,
					a.",v_recon_tounit_field," = b.to_unit,
					a.",v_recon_actiontaken_field," = b.action_tobe_taken,
					a.",v_recon_entryflag_field," = b.entry_flag,
					a.",v_recon_entryrefno_field," = b.entry_ref_no,
					a.",v_recon_iutrefno_field," = b.iut_ref_no
				");

			call pr_run_sql2(v_sql,@msg,@result);

			-- sync noniut theme in pd recon
			set v_sql = concat("update ",v_pdtran_table," as a
				inner join recon_tmp_tiuttheme as b on a.tran_gid = b.tran_gid
					and b.tranbrkp_gid = 0
					and b.noniut_theme <> ''
				set
					a.",v_recon_iutflag_field," = b.noniut_theme,
					a.",v_recon_iutvalue_field," = b.iut_value,
					a.",v_recon_closingbalance_field," = b.closing_balance,
					a.",v_recon_fromunit_field," = b.from_unit,
					a.",v_recon_tounit_field," = b.to_unit,
					a.",v_recon_actiontaken_field," = b.action_tobe_taken,
					a.",v_recon_entryflag_field," = b.entry_flag,
					a.",v_recon_entryrefno_field," = b.entry_ref_no,
					a.",v_recon_iutrefno_field," = b.iut_ref_no
				");

			call pr_run_sql2(v_sql,@msg,@result);

			-- sync noniut theme in pd recon
			set v_sql = concat("update ",v_pdtranbrkp_table," as a
				inner join recon_tmp_tiuttheme as b on a.tran_gid = b.tran_gid
					and b.tranbrkp_gid = a.tranbrkp_gid
					and b.iut_flag <> ''
				set
					a.",v_recon_iutflag_field," = b.iut_flag,
					a.",v_recon_iutvalue_field," = b.iut_value,
					a.",v_recon_closingbalance_field," = b.closing_balance,
					a.",v_recon_fromunit_field," = b.from_unit,
					a.",v_recon_tounit_field," = b.to_unit,
					a.",v_recon_actiontaken_field," = b.action_tobe_taken,
					a.",v_recon_entryflag_field," = b.entry_flag,
					a.",v_recon_entryrefno_field," = b.entry_ref_no,
					a.",v_recon_iutrefno_field," = b.iut_ref_no
				");

			call pr_run_sql2(v_sql,@msg,@result);
			
			-- pd sync end
		end loop pdrecon_loop;
		
		close pdrecon_cursor;
	end pdrecon_block;

  drop temporary table if exists recon_tmp_tiuttheme;

  set out_result = 1;
  set out_msg = 'Success';
end $$

DELIMITER ;