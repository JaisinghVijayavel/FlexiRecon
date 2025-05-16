DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_con_upd_duplicaterecords` $$
CREATE PROCEDURE `pr_con_upd_duplicaterecords`(IN pipelinecode varchar(32), IN schedulerid int)
me:BEGIN
declare v_sql text default "";
declare v_upload_mode varchar(64);
declare v_target_dsdbname varchar(64);
declare v_target_dataset varchar(64);
	
    #Get Target Database Name 
    select config_value into v_target_dsdbname from con_mst_tconfig 
	where config_name = 'target_dataset_db'
	and delete_flag = 'N';
    
	select target_dataset_code into v_target_dataset 
    from con_mst_tpipeline
	where pipeline_code = pipelinecode 
    and pipeline_status = 'Active'
	and delete_flag = 'N';
    
    select upload_mode,reject_duplicate_flag,error_mode,duplicate_mode,key_field
	into v_upload_mode,@reject_duplicate_flag,@errormode,@duplicate_mode,@keyfields 
	from con_trn_tpplfinalization
	where pipeline_code = pipelinecode
	and delete_flag = 'N';
    
    if @duplicate_mode = "Abort on Duplicates" then 
		set @reject_duplicate_flag = 'Y';
	elseif (@duplicate_mode = "Continue with Duplicates") and (v_upload_mode = "Insert or Update based on key" or v_upload_mode = "Insert or Update based on key with log") then
		set @reject_duplicate_flag = 'Y';
    end if;
        
    #Get Bcp Columns against Keyfield
    set @replace_keyfields = concat("'",replace(@keyfields,",","','"),"'");
    -- select @replace_keyfields;
    set v_sql = "";
	set v_sql = concat("select 
		group_concat(a.dataset_table_field) into @bcpcol
		from con_trn_tpplfieldmapping as b
		left join con_trn_tpplsourcefield as a on a.pipeline_code = b.pipeline_code 
		and a.sourcefield_name = b.ppl_field_name
		and a.delete_flag = 'N'
		where b. pipeline_code =  '",pipelinecode,"'
		and b.dataset_field_name in (",@replace_keyfields,")
		and (b.pipeline_code = a.pipeline_code or b.pplfieldmapping_flag = 1) 
		and b.delete_flag = 'N';");
    set @v_sql = v_sql;
    select @v_sql as sql01;
	prepare _sql from @v_sql;
	execute _sql;
    
    #Tag Duplicate Records In BCP Table
    if (@reject_duplicate_flag = 'Y') then
		drop temporary table if exists temp_duplicates; 
        set v_sql = "";
        set v_sql = concat("CREATE TEMPORARY TABLE temp_duplicates AS
		SELECT bcp_gid,",@bcpcol," FROM con_trn_tbcp
		WHERE scheduler_gid = ",schedulerid," AND status_flag = 'V' AND delete_flag = 'N'
		GROUP BY ",@bcpcol," HAVING COUNT(*) > 1;");
		set @v_sql = v_sql;
        select @v_sql as sql001;
		prepare _sql from @v_sql;
		execute _sql;
        
        #Check the Duplicate record on given file and mark as Duplicate Record
		set v_sql = "";
		set v_sql = concat("UPDATE con_trn_tbcp SET validation_remarks = 'Duplicate Record',status_flag = 'N' ");
		set v_sql = concat(v_sql,"WHERE (",@bcpcol,") IN ( ");
		set v_sql =concat(v_sql," SELECT ",@bcpcol," FROM temp_duplicates) ");
		set v_sql =concat(v_sql," AND scheduler_gid = ",schedulerid);
		set v_sql =concat(v_sql," AND status_flag = 'V'	AND delete_flag = 'N' ");
        set @v_sql = v_sql;
	    select @v_sql as sql002;
		prepare _sql from @v_sql;
	    execute _sql;
        
        if  v_upload_mode = 'Append' then
			#Check the Duplicate record on given file and target dataset then mark as Duplicate Record
			set v_sql = "";
			set v_sql = concat("UPDATE con_trn_tbcp SET validation_remarks = 'Duplicate Record',status_flag = 'N' ");
			set v_sql = concat(v_sql,"WHERE (",@bcpcol,") IN ( ");
			set v_sql =concat(v_sql," SELECT ",@keyfields," FROM ",v_target_dsdbname,".",v_target_dataset," WHERE delete_flag = 'N') ");
			set v_sql =concat(v_sql," AND scheduler_gid = ",schedulerid);
			set v_sql =concat(v_sql," AND status_flag = 'V'	AND delete_flag = 'N' ");
			set @v_sql = v_sql;
			select @v_sql as sql3;
			prepare _sql from @v_sql;
			execute _sql;
		end if;
    end if;
END $$

DELIMITER ;