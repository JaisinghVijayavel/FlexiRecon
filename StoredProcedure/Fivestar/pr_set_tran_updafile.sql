DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_tran_updafile` $$
CREATE PROCEDURE `pr_set_tran_updafile`(
  in in_file_gid int(10),
  out out_msg text,
  out out_result int(10)
  )
me:BEGIN
  declare v_file_gid int default 0;
  declare v_col_all text default '';
  declare v_count int default 0;
  declare v_xcount int default 0;
  declare v_value text default '';
  declare v_tran_date_format text default '';
  declare v_file_type char(1) default '';
  declare v_col_fieldall text default '';
  declare v_tran_column text default '';
  declare v_bcpcol_fieldnames text default '';
  declare v_tranfield_fieldall text default '';
  declare v_bcpcol_names text default '';
  declare v_sql text default '';
  declare v_filetemplate_gid int default 0;
  declare v_csv_columns int default 0;
  declare v_col_field text default '';
  declare err_flag bool default false;
  declare i int default 0;
  declare v_bcpfield_all text default '';
  declare v_tranid_validation text default '';
  declare v_tranfield_all text default '';
  declare v_header_bcp_gid int default 0;
  
  set v_file_gid=in_file_gid;
    
  if not exists(select file_gid from recon_trn_tfile
     where file_gid = v_file_gid and delete_flag = 'N') then
    set out_msg = 'File not found !';
    set out_result = 0;
    leave me;
  end if;
        
  select
    csv_columns,filetemplate_gid,tran_date_format,file_type
  into
    v_csv_columns,v_filetemplate_gid,v_tran_date_format,v_file_type
  from recon_trn_tfile
  where file_gid=v_file_gid
  and delete_flag='N';
  
	-- get bcp col
	field_loop:loop
		if i = v_csv_columns then
			leave field_loop;
		end if;

		set i=i+1;
			
		set v_col_field = concat('col',cast(i as nchar));
		
		if v_col_all = '' then
			set v_col_all = v_col_field;
		else
			set v_col_all = concat(v_col_all,',',v_col_field);
		end if;
	end loop field_loop;
	
	-- get colname
	set v_sql = concat('select bcp_gid,concat_ws(char(12),',v_col_all,') into @bcp_gid,@result from recon_trn_tbcp ',
										'where file_gid = ',cast(in_file_gid as nchar),' and delete_flag = ''N'' order by 1 limit 0,1');

	set @v_sql = v_sql;
	prepare _sql from @v_sql; 
	execute _sql;
	deallocate prepare _sql;

	set v_bcpcol_names=@result;
 
	-- get colfieldname
	call pr_get_bcpxlheaderfield(in_file_gid,v_filetemplate_gid,v_csv_columns,v_tran_date_format,
																 v_bcpfield_all,v_tranfield_all,v_header_bcp_gid);

	set @v_tran_column=v_tranfield_all;

	select if(FIND_IN_SET('tran_gid', @v_tran_column) > 0, 'tranid is present', 'tranid  is not present') into v_tranid_validation;

	if v_tranid_validation='tranid  is not present' then
		set err_flag=true;
		set out_msg='Tran Id is mandatory';
		set out_result=0;
		leave me;
	end if;

	call pr_get_colvalidation(v_tranfield_all,v_csv_columns,@out_count,@out_result);
	set v_xcount=@out_count;
	
	if v_xcount <> 0 then
		set out_msg='Duplicates Field In excel Columns';
		set out_result=1;
		leave me;
	end if;
	
	set i =0;

	delete from recon_trn_tbcp_upd where file_gid = in_file_gid;

	inserting_loop: loop
		if i = v_csv_columns then
			leave inserting_loop;
		end if;

		set i=i+1;

		set v_col_fieldall = fn_get_splitstr(v_col_all,',',i);
		set v_bcpcol_fieldnames = fn_get_splitstr(v_bcpcol_names,char(12),i);
		set v_tranfield_fieldall = fn_get_splitstr(v_tranfield_all,',',i);

		set v_sql = concat('insert into recon_trn_tbcp_upd (bcp_col,file_gid,col_name,col_field_name)
												values (',char(39),v_col_fieldall,char(39),',',char(39),in_file_gid,char(39),',',char(39),v_bcpcol_fieldnames,char(39),',',char(39),v_tranfield_fieldall,char(39),')');

		set @v_sql = v_sql;
	 
		prepare _sql from @v_sql;
		execute _sql;
		deallocate prepare _sql;
	end loop inserting_loop;

	call pr_set_bcp_updvalue(v_file_gid,v_csv_columns,v_file_type,v_col_all,v_tranfield_all,@out_msg1,@out_result1);

	set out_msg=@out_msg1;
	set out_result=@out_result1;
END $$

DELIMITER ;