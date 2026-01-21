DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_multiresultset` $$
CREATE PROCEDURE `pr_run_multiresultset`
(
  in in_recon_code text,
  in in_report_code text,
  in in_reporttemplate_code text,
  in in_condition longtext,
  in in_report_param text,
  in in_job_gid bigint,
  in in_archival_code varchar(32),
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
  /*
    Created By :
    Created Date :

    Updated By : Vijayavel
    updated Date : 21-01-2026

    Version : 1
  */

	declare v_count int default 0;
	declare v_increment int default 1;
	declare v_report_code_es text;
	declare v_reporttemplate_code_es text;
	
	declare v_sql text;
	declare v_query_sql longtext;
	declare v_sql_condition longtext;
	
	declare v_table_name text default '';
	declare v_rpt_table_name text default '';
	declare v_sorting_field text default '';
	declare v_report_condition text default '';
	declare v_job_gid bigint default 0;
	declare v_sp_name text default '';
	declare v_sortby_code varchar(32);
	declare v_recon_code text default '';
	declare v_report_code varchar(32);
	declare v_report_exec_type char(1) default '';
	declare v_dataset_db_name text default '';
	declare  v_table_prefix text default '';
	
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE,
		@errno = MYSQL_ERRNO, @text = MESSAGE_TEXT;

		SET @full_error = CONCAT("ERROR ", @errno, " (", @sqlstate, "): ", @text);

		ROLLBACK;
   
		call pr_upd_job(in_job_gid,'F',@text,@msg,@result);

		set out_msg = @full_error;
		set out_result = 0;

		SIGNAL SQLSTATE '99999' SET
		MYSQL_ERRNO = @errno,
		MESSAGE_TEXT = @text;
	END;
	
	set v_recon_code = in_recon_code;
	set v_report_code = in_report_code;
	set v_job_gid = in_job_gid;
	set v_table_prefix = fn_get_recontableprefix(in_archival_code,in_recon_code);
  	
	drop temporary table if exists tmp_resultset;
	drop temporary table if exists tmp_report_conditions;
	
	create temporary table tmp_report_conditions (report_code varchar(255),report_name varchar(255),sql_condition text);
	 
	set in_condition = replace(in_condition,char(12),char(39));
	set in_condition = replace(in_condition,concat(char(39),char(39)),char(39));

	call pr_run_multiresultset_parse_json_array(in_condition);

	create temporary table tmp_resultset
	(
		id int primary key auto_increment,
		report_code text,
		sheet_name varchar(255) not null,
		report_exec_type varchar(5) not null,
		src_report_code varchar(255),
		sp_name text,
		resultset_order decimal(12,2),
		query longtext,
		resultset_name varchar(255),
		resultset_code varchar(32)
	);
	
	insert into tmp_resultset
	(
		sheet_name,report_exec_type,src_report_code,sp_name,resultset_order,query,resultset_name,resultset_code
	)
	select
		sheet_name,resultset_exec_type,src_report_code,sp_name,resultset_order,query,resultset_name,reporttemplateresultset_code
	from recon_mst_treporttemplateresultset
	where reporttemplate_code = in_reporttemplate_code
	and delete_flag = 'N'
	order by resultset_order;
    
	select count(*) into v_count from tmp_resultset;
	 
	WHILE(v_count >= v_increment)DO
		select 
			report_exec_type,src_report_code,sp_name,query,resultset_name,resultset_code
		into 
			@report_exec_type_code,@src_report_code,@sp_name,@query ,@resultset_name,@resultset_code
		from tmp_resultset 
		where id = v_increment;
		
    update recon_trn_tjob set 
			job_remark=@resultset_name 
		where job_gid=v_job_gid;
		
		if @report_exec_type_code = "R" then
			select 
				r.report_code,t.reporttemplate_code,r.report_exec_type,r.sp_name,r.table_name,r.rpt_table_name
			into 
				v_report_code_es,v_reporttemplate_code_es,v_report_exec_type,v_sp_name,v_table_name,v_rpt_table_name
			from recon_mst_treport as r
			inner join recon_mst_treporttemplateresultset as t on r.report_code = t.src_report_code
				and t.active_status = 'Y'
				and t.delete_flag = 'N' 
			where t.reporttemplateresultset_code = @resultset_code
			and t.reporttemplate_code = in_reporttemplate_code
			and r.active_status = 'Y'	
			and r.delete_flag = 'N';
			
      set v_sql_condition = '';
            
      select
				sql_condition into v_sql_condition 
			from tmp_report_conditions
      where report_name=@resultset_name;
			
			set v_sql_condition = ifnull(v_sql_condition,'');
			set v_sp_name = ifnull(v_sp_name,'');

			if v_rpt_table_name <> '' then
				set v_table_name = v_rpt_table_name;
  		end if;
			
			select
				group_concat(concat(ifnull(b.field_name,if(instr(a.report_field,'.') = 0,a.report_field,SPLIT(a.report_field,'.',2))),' ',a.sorting_type))
			into
				v_sorting_field
			from recon_mst_treporttemplatesorting as a
			inner join recon_mst_tsystemfield as b on b.report_field_name = a.report_field
				and b.delete_flag = 'N'
			where a.reporttemplate_code = in_reporttemplate_code
      and a.reporttemplateresultset_code=@resultset_code
			and b.table_name = v_table_name
			and a.active_status = 'Y'
			and a.delete_flag = 'N'
			order by a.sorting_order;			
			
			set v_sorting_field = ifnull(v_sorting_field,'');

		  if v_sorting_field <> '' then
		    set v_sorting_field = concat('order by ',v_sorting_field);
		  end if;
		  
		  if v_report_exec_type = 'S' then
		  	call pr_run_sp('',v_recon_code,v_sp_name,v_job_gid,v_increment,v_sql_condition,v_sorting_field,in_user_code,@msg,@result);

		    set v_report_condition = concat(' and job_gid = ', cast(v_job_gid as nchar) ,' ');
		    set v_report_condition = concat(v_report_condition,' and rptsession_gid = ',cast(v_increment as nchar),' ');

		    if v_job_gid = 0 then
		      set v_report_condition = concat(v_report_condition," and user_code = '",in_user_code,"' and rptsession_gid = 0 ");
		    end if;

		    set v_report_condition = concat(v_report_condition,' ',v_sorting_field,' ');

		    call pr_run_tablequery(in_reporttemplate_code,
		                           v_recon_code,
		                           v_report_code_es,
		                           v_table_name,
		                           v_report_condition,
		                           v_job_gid,
		                           false,
		                           '',
		                           in_user_code,
                               @resultset_code,
                               @msg,@result);
		    
			elseif v_report_exec_type = 'D' then
				set v_dataset_db_name = fn_get_configvalue('dataset_db_name');
				
				if in_archival_code = '' then
		      if v_dataset_db_name <> '' then
		        set v_table_name = concat(v_dataset_db_name,'.',v_report_code_es);
		      else
		        set v_table_name = v_report_code_es;
		      end if;
		    else
		      set v_table_name = concat(v_table_prefix,v_report_code_es);
		    end if;
			
		    call pr_run_tablequery(in_reporttemplate_code,
		                           v_recon_code,
		                           v_report_code_es,
		                           v_table_name,
		                           v_sql_condition,
		                           v_job_gid,
		                           false,
		                           '',
		                           in_user_code,
                               @resultset_code,
                               @msg,@result);
	    end if;
		elseif @report_exec_type_code = "S" then
			set v_sql = concat('call ',@sp_name);
			
			set v_sql = REPLACE(v_sql, '$CURDATE$', fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_condition,'$CURDATE$',in_user_code) );
			
			set v_sql = REPLACE(v_sql, '$CURDATETIME$',  fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_condition,'$CURDATETIME$',in_user_code) );
			
			set v_sql = REPLACE(v_sql, '$RECONCODE$',  fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_condition,'$RECONCODE$',in_user_code) );
			
			set v_sql = REPLACE(v_sql, '$USERCODE$',  fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_condition,'$USERCODE$',in_user_code) );
			
			set v_sql = REPLACE(v_sql, '$CYCLEDATE$',  fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_condition,'$CYCLEDATE$',in_user_code) );
			
			set v_sql = REPLACE(v_sql, '$RECONCYCLEDATE$', fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_condition,'$RECONCYCLEDATE$',in_user_code) );
			
			set v_sql = REPLACE(v_sql, '$RECONCLOSUREDATE$',  fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_condition,'$RECONCLOSUREDATE$',in_user_code) );

			set v_sql = REPLACE(v_sql, '$ARCHIVALCODE$',  fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_condition,'$ARCHIVALCODE$',in_user_code) );
			
			set @sql = v_sql;
			prepare mulltisp_stmt from @sql;
			execute mulltisp_stmt;
			deallocate prepare mulltisp_stmt;
		elseif @report_exec_type_code = "Q" then
			set v_query_sql = @query;

			set v_query_sql = REPLACE(v_query_sql, '$CURDATE$',  fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_condition,'$CURDATE$',in_user_code) );
			
			set v_query_sql = REPLACE(v_query_sql, '$CURDATETIME$',  fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_condition,'$CURDATETIME$',in_user_code) );
			
			set v_query_sql = REPLACE(v_query_sql, '$RECONCODE$',  fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_condition,'$RECONCODE$',in_user_code) );
			
			set v_query_sql = REPLACE(v_query_sql, '$USERCODE$', fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_condition,'$USERCODE$',in_user_code) );
			
			set v_query_sql = REPLACE(v_query_sql, '$CYCLEDATE$',  fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_condition,'$CYCLEDATE$',in_user_code) );
			
			set v_query_sql = REPLACE(v_query_sql, '$RECONCYCLEDATE$',  fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_condition,'$RECONCYCLEDATE$',in_user_code) );
			
			set v_query_sql = REPLACE(v_query_sql, '$RECONCLOSUREDATE$',  fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_condition,'$RECONCLOSUREDATE$',in_user_code) );
			
			set v_query_sql = REPLACE(v_query_sql, '$ARCHIVALCODE$', fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_condition,'$ARCHIVALCODE$',in_user_code) );

			set @querysql = v_query_sql;
			prepare mulltiquery_stmt from @querysql;
			execute mulltiquery_stmt;
			deallocate prepare mulltiquery_stmt;
		end if;

		set v_increment = v_increment+1;
	END WHILE;
END $$

DELIMITER ;