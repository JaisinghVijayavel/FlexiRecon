DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_treporttemplateresultset` $$
CREATE PROCEDURE `pr_recon_mst_treporttemplateresultset`
(
	in in_reporttemplate_code varchar(32),
	in in_resultset_id int,
	in in_report_code varchar(255),
	in in_resultset_name varchar(255),
	in in_sheet_name varchar(255),
	in in_resultset_order decimal(18,2),
	in in_exec_type char,
	in in_exec_type_data longtext,
	in in_user_code varchar(32),
	in in_action varchar(32),
	out out_msg text,
	out out_result int
)
me:BEGIN
	declare v_reporttemplate_code text default '';
	declare v_recon_code text default '';

  DECLARE exit handler FOR SQLEXCEPTION 
	BEGIN
    GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE,
    @errno = MYSQL_ERRNO, @text = MESSAGE_TEXT;

    call pr_ins_errorlog('system','localhost','sp','pr_recon_mst_treporttemplateresultset',@text,@msg,@result);

		if(in_action = 'Checkquery') then
			set out_msg =out_msg;
			set out_result = 1;
		elseif(in_action = 'CheckSP') then
			/*SELECT concat('Error in executing SP - ' , MESSAGE_TEXT )
					into out_msg 
					FROM information_schema.innodb_trx LIMIT 1; */
			set out_msg= out_msg ; -- 'Error in executing SP';
			set out_result = 1;
		elseif(in_action = 'Insert') then
			/*SELECT concat('Error in executing SP - ' , MESSAGE_TEXT )
					into out_msg
					FROM information_schema.innodb_trx LIMIT 1; */
			set out_msg= out_msg ; -- 'Error in executing SP';
			set out_result = 1;
		else
			SELECT  
				MESSAGE_TEXT into out_msg
			FROM information_schema.innodb_trx LIMIT 1; 
		end if;
	END;
	
	-- get recon code
	select 
		recon_code into v_recon_code 
	from recon_mst_treporttemplate
	where reporttemplate_code = in_reporttemplate_code
	and active_status = 'Y' 
	and delete_flag = 'N';
	
	set v_recon_code = ifnull(v_recon_code,'');
	
	if(in_action = 'Insert')then
		set out_msg = "-";
		set out_result = 0;
		
		if exists(select 1 from recon_mst_treporttemplateresultset 
				where resultset_order = in_resultset_order
				and reporttemplate_code = in_reporttemplate_code 
				and active_status = 'Y' 
				and delete_flag = 'N') then
			set out_msg = "Resultset order cannot be duplicate !";
			set out_result = 1;
			leave me;
		end if;
		
		if not exists(select 1 from recon_mst_treporttemplateresultset 
				where sheet_name = in_sheet_name
				and reporttemplate_code = in_reporttemplate_code 
				and active_status = 'Y' 
				and delete_flag = 'N' ) then
							
			if not exists(select 1 from recon_mst_treporttemplateresultset 
					where resultset_name = in_resultset_name
					and reporttemplate_code = in_reporttemplate_code 
					and active_status='Y' 
					and delete_flag='N') then
				insert into recon_mst_treporttemplateresultset
				(
					reporttemplate_code,
					resultset_name,
					resultset_order,
					sheet_name,
					resultset_exec_type,
					src_report_code,
					sp_name,
					query,
					active_status,
					insert_date,
					insert_by
				)
				values
				(
					in_reporttemplate_code,
					in_resultset_name,
					in_resultset_order,
					in_sheet_name,
					in_exec_type,
					case when in_exec_type = 'R' then in_exec_type_data else '' end,
					case when in_exec_type = 'S' then in_exec_type_data else '' end,
					case when in_exec_type = 'Q' then in_exec_type_data else '' end,
					'Y',
					now(),
					in_user_code
				);


				SET @newId = LAST_INSERT_ID();
				
				update recon_mst_treporttemplateresultset set 
					reporttemplateresultset_code = concat('RST',@newId ) 
				where reporttemplateresultset_gid=@newId ;
				
				set out_msg = "Record Inserted Succesfully..!";
				set out_result = 0;

				if in_exec_type = 'R' then
					insert into recon_mst_treporttemplatefilter
					(
						reporttemplate_code,
						reporttemplateresultset_code,
						filter_seqno,
						report_field,
						filter_criteria,
						filter_value,
						open_parentheses_flag,
						close_parentheses_flag,
						join_condition,
						system_flag,
						active_status
					)
					select 
						in_reporttemplate_code as reporttemplate_code,
						concat('RST',@newId ),
						filter_seqno,
						report_field,
						filter_criteria,
						filter_value,
						open_parentheses_flag,
						close_parentheses_flag,
						join_condition,
						'Y'as system_flag,
						active_status 
					from recon_mst_treportfilter 
					where report_code = in_exec_type_data
					and active_status = 'Y' 
					and delete_flag='N';
				end if;
			else
				set out_msg = "Resultset Name cannot be duplicate !";
				set out_result = 1;
			end if;
		else
			set out_msg = "Sheet Name cannot be duplicate !";
			set out_result = 1;
		end if; 
	elseif(in_action = 'Update')then
		if exists(select 1 from recon_mst_treporttemplateresultset
				where resultset_order = in_resultset_order
				and reporttemplate_code = in_reporttemplate_code
        and reporttemplateresultset_gid <> in_resultset_id
				and active_status='Y' 
				and delete_flag='N' ) then
			set out_msg = "Resultset order cannot be duplicate !";
			set out_result = 1;
      leave me;
    end if;
		
		if not exists(select 1 from recon_mst_treporttemplateresultset 
				where sheet_name = in_sheet_name
        and reporttemplate_code = in_reporttemplate_code 
        and reporttemplateresultset_gid != in_resultset_id
				and active_status='Y' 
				and delete_flag='N') then
                 
			if not exists(select 1 from recon_mst_treporttemplateresultset 
					where resultset_name = in_resultset_name
				  and reporttemplate_code = in_reporttemplate_code 
				  and reporttemplateresultset_gid != in_resultset_id
					and active_status = 'Y' 
					and delete_flag='N') then
				update recon_mst_treporttemplateresultset set 
					resultset_name = in_resultset_name, 
					resultset_order = in_resultset_order,
					sheet_name = in_sheet_name,
					resultset_exec_type = in_exec_type,
					src_report_code = CASE WHEN in_exec_type = 'R' THEN in_exec_type_data ELSE NULL END,
					sp_name = CASE WHEN in_exec_type = 'S' THEN in_exec_type_data ELSE NULL END,
					query = CASE WHEN in_exec_type = 'Q' THEN in_exec_type_data ELSE NULL END,
					update_date = now(),
					update_by = in_user_code
				where reporttemplateresultset_gid = in_resultset_id;
				
				set out_msg = "Record Updated Succesfully..!";
				set out_result = 0;
			else
				set out_msg = "Resultset Name cannot be duplicate !";
				set out_result = 1;    
			end if;                    
    else
			set out_msg = "Sheet Name cannot be duplicate !";
			set out_result = 1;
    end if;   
	elseif(in_action = 'Delete')then
		select 
			reporttemplate_code into v_reporttemplate_code 
		from recon_mst_treporttemplateresultset 
		where reporttemplateresultset_gid = in_resultset_id;

		update recon_mst_treporttemplateresultset set 
			delete_flag = 'Y',
			active_status = 'N'
		where reporttemplateresultset_gid = in_resultset_id;
            
    update recon_mst_treporttemplate set 
			active_status = 'D'
		where reporttemplate_code = v_reporttemplate_code;
            
		set out_msg = "Record Deleted Succesfully..!";
		set out_result = 0;
	elseif(in_action = 'Get')then
		select 
			reporttemplateresultset_gid as reportresultset_gid,
			resultset_order,
			sheet_name,
			resultset_name,
			case when resultset_exec_type = 'R' then src_report_code  
			else concat(reporttemplateresultset_gid,'-',resultset_name) end as report_code,
			IF
			(
				resultset_exec_type = 'R', 
				(select report_desc from recon_mst_treport where report_code=src_report_code),
				IF(resultset_exec_type = 'S', sp_name, Query)
			) AS report_name,
			resultset_exec_type as exec_type_code,
			IF
			(
				resultset_exec_type = 'R', 
				'Report',
				IF(resultset_exec_type = 'S', 'SP', 'Query')
			) AS exec_type,
			concat(resultset_name,' - ', IF(resultset_exec_type = 'R', 'Report',IF(resultset_exec_type = 'S', 'SP', 'Query'))) as resultset_dropname,
			reporttemplateresultset_code
		from recon_mst_treporttemplateresultset 
		where reporttemplate_code = in_reporttemplate_code 
		and active_status = 'Y' 
		and delete_flag = 'N' 
		order by resultset_order;
		
		set out_msg = "Record Fetched Succesfully..!";
		set out_result = 0;
	elseif(in_action = 'Checkquery')then
		SET @q_upper = UPPER(TRIM(in_exec_type_data));
    set out_msg = "Query Validation Failed..!";
		
		IF LEFT(@q_upper, 6) != 'SELECT' THEN
			set out_msg ='Only SELECT queries are allowed!';
			SIGNAL SQLSTATE '45000'
			SET MESSAGE_TEXT = 'Only SELECT queries are allowed!';
		END IF;
		
		-- Block any DML/DDL keywords even inside subqueries
		IF @q_upper REGEXP '\\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|REPLACE|EXEC|CALL)\\b' THEN
			set out_msg ='Query contains restricted keywords!';
      SIGNAL SQLSTATE '45000'
			SET MESSAGE_TEXT = 'Query contains restricted keywords!';
    END IF;

		-- Block multiple statements
		IF @q_upper LIKE '%;%' THEN
			set out_msg ='Multiple statements not allowed!';
			SIGNAL SQLSTATE '45000'
			SET MESSAGE_TEXT='Multiple statements not allowed!';
		END IF;
		
		set in_exec_type_data = fn_get_reconstaticcondition('',v_recon_code,in_exec_type_data,in_user_code);

		set @query=in_exec_type_data;
		prepare stmt from @query;
		execute stmt;
		deallocate prepare stmt;

		set out_msg = "Query Validated Succesfully..!";
		set out_result = 0;
		
    if fn_recon_check_staticfield(in_exec_type_data) ='Valid' then
			set out_msg = "Query Validated Succesfully..!";
			set out_result = 0;
    else
			set out_msg = "Static field mismatch..!";
			set out_result = 0;
			SIGNAL SQLSTATE '45000'
			SET MESSAGE_TEXT = 'Query contains restricted keywords!';
    end if;
	elseif(in_action ='checkSP')then
		set out_msg=  'Error in executing SP';
		set in_exec_type_data = fn_get_reconstaticcondition('',v_recon_code,in_exec_type_data,in_user_code);

		SET @dyn_sql = CONCAT('CALL ', in_exec_type_data,';');

    select @dyn_sql;
		PREPARE stmt FROM @dyn_sql;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;

		set out_msg = "SP Validated Succesfully..!";
		set out_result = 0;

    if fn_recon_check_staticfield(@dyn_sql) ='Valid' then
			set out_msg = "SP Validated Succesfully..!";
			set out_result = 0;
    else
			set out_msg = "Static field mismatch..!";
			set out_result = 0;
			SIGNAL SQLSTATE '45000'
			SET MESSAGE_TEXT = 'Query contains restricted keywords!';
		end if;
  elseif(in_action = 'GET_STATIC_FIELDS')then
		-- select 1;
    call pr_get_reconstaticfields();
    set out_msg = "output";
	  set out_result = 0;
	end if;
END $$

DELIMITER ;