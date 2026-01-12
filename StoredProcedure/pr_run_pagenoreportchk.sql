DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_pagenoreportchk` $$
CREATE PROCEDURE `pr_run_pagenoreportchk`(
  in in_reporttemplate_code varchar(32),
  in in_recon_code varchar(32),
  in in_report_code varchar(32),
  in in_rptsession_gid int,
  in in_condition text,
  in in_page_no int,
  in in_page_size int,
  in in_tot_records int,
  out out_msg text,
  out out_result int
)
me:BEGIN
  /*
    Created By : Vijayavel
    Created Date :

    Updated By : Vijayavel
    updated Date : 12-01-2026

    Version : 1
  */

  declare v_recon_code text default '';
  declare v_report_code text default '';
  declare v_reporttemplateresultset_code varchar(32) default '';
  declare v_recon_code_field text default '';
  declare v_recon_flag text default '';
  declare v_multi_recon_flag text default '';
  declare v_table_name text default '';
  declare v_rpt_table_name text default '';
  declare v_condition text default '';
  declare v_start_rec_no int default 0;
  declare v_sortby_code varchar(32);
  declare v_sorting_field text default '';

  declare v_rptsession_gid int default 0;

  declare v_report_exec_type text default '';
  declare v_dataset_db_name text default '';

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE,
    @errno = MYSQL_ERRNO, @text = MESSAGE_TEXT;

    SET @full_error = CONCAT("ERROR ", @errno, " (", @sqlstate, "): ", @text,' ',err_msg);

    call pr_ins_errorlog('','','sp','pr_run_pagenoreport',@full_error,@msg,@result);

    set out_msg = @text;
    set out_result = 0;

    SIGNAL SQLSTATE '99999' SET
    MYSQL_ERRNO = @errno,
    MESSAGE_TEXT = @text;
  END;

  set in_reporttemplate_code = ifnull(in_reporttemplate_code,'');

  -- get recon code
  if in_reporttemplate_code <> '' then
    -- get report code
    select
      a.recon_code,
      b.reporttemplateresultset_code,
      b.src_report_code
    into
      v_recon_code,
      v_reporttemplateresultset_code,
      v_report_code
    from recon_mst_treporttemplate as a
    inner join recon_mst_treporttemplateresultset as b on a.reporttemplate_code = b.reporttemplate_code
      and b.delete_flag = 'N'
    where a.reporttemplate_code = in_reporttemplate_code
    and a.delete_flag = 'N'
    order by b.resultset_order limit 1;

    set v_recon_code = ifnull(v_recon_code,'');
    set v_report_code = ifnull(v_report_code,'');
    set v_reporttemplateresultset_code = ifnull(v_reporttemplateresultset_code,'');

    -- get sorting field
    select
      group_concat(concat(report_field,' ',sorting_type))
    into
      v_sortby_code
    from recon_mst_treporttemplatesorting
    where reporttemplate_code = in_reporttemplate_code
    and reporttemplateresultset_code = v_reporttemplateresultset_code
    order by sorting_order;

    set v_sortby_code = lower(ifnull(v_sortby_code,'asc'));
  else
    set v_recon_code = ifnull(in_recon_code,'');
    set v_report_code = ifnull(in_report_code,'');
    set v_sortby_code = 'asc';
  end if;

  if v_sortby_code = 'asc' then
    set v_sortby_code = '';
  end if;

  -- get report code
	select
		report_exec_type,
    recon_code_field,
		table_name,
    rpt_table_name,
    recon_flag,
    multi_recon_flag
	into
		v_report_exec_type,
    v_recon_code_field,
		v_table_name,
    v_rpt_table_name,
    v_recon_flag,
    v_multi_recon_flag
	from recon_mst_treport
	where report_code = in_report_code
	and delete_flag = 'N';

  set v_condition = ifnull(v_condition,'');
  set v_recon_flag = ifnull(v_recon_flag,'N');
  set v_multi_recon_flag = ifnull(v_multi_recon_flag,'N');

  set v_table_name = ifnull(v_table_name,'');
  set v_rpt_table_name = ifnull(v_rpt_table_name,'');

  if v_rpt_table_name <> '' then
    set v_table_name = v_rpt_table_name;
  end if;

  -- session
  if exists(select rptsession_gid from recon_trn_treportsession
     where rptsession_gid = in_rptsession_gid
     and delete_flag = 'N') and v_report_exec_type = 'S' then
    select
      b.table_name,b.rpt_table_name
    into
      v_table_name,v_rpt_table_name
    from recon_trn_treportsession as a
    inner join recon_mst_treport as b on a.report_code = b.report_code and b.delete_flag = 'N'
    where a.rptsession_gid = in_rptsession_gid
    and a.delete_flag = 'N';

    set v_table_name = ifnull(v_table_name,'');
    set v_rpt_table_name = ifnull(v_rpt_table_name,'');

    if v_rpt_table_name <> '' then
      set v_table_name = v_rpt_table_name;
    end if;

		-- sort order
    if in_reporttemplate_code <> '' then
			select
				group_concat(concat(ifnull(b.field_name,if(instr(a.report_field,'.') = 0,a.report_field,SPLIT(a.report_field,'.',2))),' ',v_sortby_code))
			into
				v_sorting_field
			from recon_mst_treporttemplatesorting as a
			left join recon_mst_tsystemfield as b on b.report_field_name = a.report_field
				and b.table_name = v_table_name
				and b.delete_flag = 'N'
			where a.reporttemplate_code = in_reporttemplate_code
			and a.active_status = 'Y'
			and a.delete_flag = 'N'
			order by a.sorting_order;
    else
			select
				group_concat(concat(ifnull(b.field_name,if(instr(a.report_field,'.') = 0,a.report_field,SPLIT(a.report_field,'.',2))),' ',v_sortby_code))
			into
				v_sorting_field
			from recon_mst_treportsorting as a
			left join recon_mst_tsystemfield as b on b.report_field_name = a.report_field
				and b.table_name = v_table_name
				and b.delete_flag = 'N'
			where a.report_code = in_report_code
			and a.active_status = 'Y'
			and a.delete_flag = 'N'
			order by a.sorting_order;
    end if;

		set v_sorting_field = ifnull(v_sorting_field,'');

		if v_sorting_field <> '' then
			set v_sorting_field = concat('order by ',v_sorting_field);
		end if;
  elseif v_report_exec_type = 'T' or v_report_exec_type = 'D' then
    if v_report_exec_type = 'D' then
      set v_dataset_db_name = fn_get_configvalue('dataset_db_name');

      if v_dataset_db_name <> '' then
        set v_table_name = concat(v_dataset_db_name,'.',in_report_code);
      else
        set v_table_name = in_report_code;
      end if;
    end if;
  else
    set out_msg = 'Invalid session';
    set out_result = 0;

    leave me;
  end if;

  set v_table_name = ifnull(v_table_name,'');

  if v_table_name = '' then
    set out_msg = 'Invalid table name';
    set out_result = 0;

    leave me;
  end if;

  set v_start_rec_no = (in_page_no - 1)*in_page_size;

  if v_report_exec_type = 'S' then
    set v_condition = concat('and rptsession_gid = ',cast(in_rptsession_gid as nchar) ,' ');
  else
    set v_condition = ifnull(in_condition,'');

    if v_recon_code_field <> '' and v_recon_flag = 'Y' and v_multi_recon_flag = 'N' then
      set v_condition = concat(' and ',v_recon_code_field,' = ',char(34),v_recon_code,char(34),' ', v_condition);
    end if;
  end if;

  set v_condition = concat(v_condition,' ',v_sorting_field,' limit ',cast(v_start_rec_no as nchar),',',cast(in_page_size as nchar),' ');

  select in_reporttemplate_code,
                         v_recon_code,
                         v_report_code,
                         v_table_name,
                         v_condition,
                         v_reporttemplateresultset_code;

  call pr_run_tablequery(in_reporttemplate_code,
                         v_recon_code,
                         v_report_code,
                         v_table_name,
                         v_condition,
                         0,
                         false,
                         '',
                         '',
                         v_reporttemplateresultset_code,@msg,@result);

  set out_msg = @msg;
  set out_result = @result;
end $$

DELIMITER ;