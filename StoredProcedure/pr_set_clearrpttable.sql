DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_clearrpttable` $$
CREATE PROCEDURE `pr_set_clearrpttable`()
me:BEGIN
	declare v_table_name text default '';
	declare v_sql text default '';

	truncate recon_trn_tpreview;
	truncate recon_trn_tpreviewdtl;
	truncate con_trn_tbcp;
	truncate recon_rpt_tko;
	truncate recon_rpt_ttran;
	truncate recon_rpt_ttranbrkp;
	truncate recon_rpt_ttranwithbrkp;
	truncate recon_rpt_tmanualpost;
	truncate recon_rpt_tmanualmatch;
	truncate recon_rpt_tpreview;
	truncate recon_rpt_tdataset;
  truncate recon_rpt_terrorlog;
  truncate recon_rpt_tjob;
  truncate recon_rpt_taccbal;

  drop temporary table if exists recon_tmp_t1table;

  CREATE TEMPORARY TABLE recon_tmp_t1table(
    table_name varchar(128) not null,
    PRIMARY KEY (table_name)
  ) ENGINE = MyISAM;

  insert into recon_tmp_t1table(table_name)
    SELECT table_name FROM information_schema.tables
    WHERE table_schema=database()
    and (table_name like '%_RPT_AMT_MATCHED'
    or table_name like '%_RPT_EXCP_WITHBRKP'
    or table_name like '%_filter_criteria');

	-- table block
	table_block:begin
		declare table_done int default 0;
		declare table_cursor cursor for
		select table_name from recon_tmp_t1table;
		declare continue handler for not found set table_done=1;

		open table_cursor;

		table_loop: loop
			fetch table_cursor into v_table_name;
			if table_done = 1 then leave table_loop; end if;

      if v_table_name like '%_filter_criteria' then
        -- drop table
        set v_sql = concat("drop table ",v_table_name);
      elseif v_table_name like '%_RPT_AMT_MATCHED' or v_table_name like '%_RPT_EXCP_WITHBRKP' then
        -- truncate table
        set v_sql = concat("truncate ",v_table_name);
      end if;

			call pr_run_sql1(v_sql,@result,@msg);
		end loop table_loop;
		close table_cursor;
	end table_block;

  drop temporary table if exists recon_tmp_t1table;
end $$

DELIMITER ;