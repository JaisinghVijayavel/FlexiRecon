DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_pdsummary` $$
CREATE PROCEDURE `pr_run_pdsummary`
(
  in in_recon_code varchar(32),
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_report_code_ko text;
  declare v_reporttemplate_code_ko text;
	declare v_report_code_es text;
  declare v_reporttemplate_code_es text;
  declare v_closing_balance_conditon text;
  declare v_closingbalance_table_name text;
	declare v_unitmaster_table_name text;
  declare v_dataset_db_name text;

  -- fetching reportcode and report template code for Custom report Exception Summary
	select
		report_code,reporttemplate_code
	into
		v_report_code_es,v_reporttemplate_code_es
  from recon_mst_treporttemplate
  where recon_code = in_recon_code
  and reporttemplate_name = 'Exception Summary'
  and active_status = 'Y'
	and delete_flag = 'N';

  call pr_run_dynamicreport(v_reporttemplate_code_es, in_recon_code,v_report_code_es,
    'Transaction Exception With Breakp','and a.scheduler_gid > 0 ', false, '', '', in_user_code, @out_msg, @out_result);


	-- RE168_RPT_EXCP_WITHBRKP
	/* set @query1 =concat('select * from ',in_recon_code,'_RPT_EXCP_WITHBRKP');
    prepare stmt from @query1;
	execute stmt;
    deallocate prepare stmt; */

    -- fetching reportcode and report template code for Custom report KO
	select
		report_code,reporttemplate_code
	into
		v_report_code_ko,v_reporttemplate_code_ko
  from recon_mst_treporttemplate
  where recon_code = in_recon_code
  and reporttemplate_name = 'KO Report - Formatted'
  and active_status = 'Y'
	and delete_flag = 'N';

	-- KO formatted
	call pr_run_dynamicreport(v_reporttemplate_code_ko, in_recon_code,v_report_code_ko,
        in_recon_code, "AND  a.ko_gid > '0' ", false, '', '', in_user_code, @out_msg, @out_result);

	-- pd closing balance v_closing_balance_conditon by filtering based on PD location
  select fn_get_configvalue('dataset_db_name')into v_dataset_db_name;

	select
		dataset_code
	into
		v_unitmaster_table_name
	from recon_mst_tdataset
  where dataset_name='Unit Master'
  and delete_flag = 'N';

  set @colname='col2';

  if v_dataset_db_name !='' then
    set v_unitmaster_table_name =concat(v_dataset_db_name,'.',v_unitmaster_table_name);
	end if;

   -- select concat('And col1 =',char(39),col2,char(39),' AND  scheduler_gid > 0')   from DS276 where col4 = 'RE168' and delete_flag = 'N' ;

	set @query1 =concat('select concat(',char(39),'And col1 =',char(39),' ,char(39),',@colname,',char(39),', char(39),' AND  scheduler_gid > ',0,char(39),')',  ' into @closing_balance_conditon  from ',v_unitmaster_table_name ,
    ' where col4 = ',char(39),in_recon_code,char(39),' and delete_flag = ''N'' ');

  -- select @query1;
  prepare stmt from @query1;
	execute stmt;
	deallocate prepare stmt;

	-- select concat('And col1 = ',char(39),col2 ,char(39),' AND  scheduler_gid > ',char(39),'0',char(39)) into v_closing_balance_conditon  from DS276 where col4 = in_recon_code and delete_flag = 'N';

  set v_closing_balance_conditon=@closing_balance_conditon;

	select
		T1.dataset_code
	into
    v_closingbalance_table_name from recon_mst_trecondataset T1,recon_mst_tdataset T2
	where T1.recon_code=in_recon_code
	and T1.dataset_code=T2.dataset_code
  and
  (
    T2.dataset_name like 'PD Recon - Closing Balance%' or
    T2.dataset_name like 'Closing Balance%' or
    T2.dataset_name like 'ClosingBalance%'
  )
  and T1.active_status='Y'
  and T1.delete_flag='N'
  and T2.active_status='Y'
  and T2.delete_flag='N' limit 1;

  /*
	if v_dataset_db_name !='' then
		set v_closingbalance_table_name =concat(v_dataset_db_name,'.',v_closingbalance_table_name);
	end if;
  */

  -- select v_closingbalance_table_name,v_closing_balance_conditon;

  -- v_unitmaster_table_name DS255
	call pr_run_dynamicreport('', in_recon_code,v_closingbalance_table_name, 'PD Recon - Closing Balance',
		v_closing_balance_conditon, false, '', '', in_user_code, @out_msg, @out_result);

	-- leave me;
	-- RE168_RPT_AMT_MATCHED
	set @query1 =concat('select * from ',in_recon_code,'_RPT_AMT_MATCHED');

	prepare stmt from @query1;
	execute stmt;
	deallocate prepare stmt;
end $$

DELIMITER ;