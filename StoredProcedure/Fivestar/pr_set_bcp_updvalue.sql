DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_bcp_updvalue` $$
CREATE PROCEDURE `pr_set_bcp_updvalue`(
  in in_file_gid int,
  in in_csv_columns int,
  in in_file_type char(1),
  in in_col_all text,
  in in_tran_fieldall text,
  out out_msg1 text,
  out out_result1 int
 )
me:BEGIN

  declare v_file_gid int default 0;
  declare v_filetemplate_gid int default 0;
  declare v_tran_gid int default 0;
  declare v_tran_gid_all text default '';
  declare v_field_type text default '';
  declare v_tran_field text default '';
  declare v_col_field text default '';
  declare v_dateval text default '';
  declare v_blank_result text default '';
  declare v_blank_resultset text default '';
  declare v_bcp_gid_all text default '';
  declare v_totaltran_count int default 0;
  declare err_flag bool default false;
  declare v_totalcol_count int default 0;
  declare v_bcp_gid int default 0;
  declare v_header_bcp_gid int default 0;
  declare v_sql text default '';
  declare v_sql1 text default '';
  declare i int default 0;
  declare j int default 0;
  declare v_col_all text default '';
  declare v_mandatory_flag text default '';

  set v_file_gid=in_file_gid;
  set v_col_all =in_col_all;

  -- get header gid
  select
    bcp_gid into v_header_bcp_gid
  from recon_trn_tbcp
  where file_gid = in_file_gid
  order by 1 asc
  limit 1;

  set v_header_bcp_gid = ifnull(v_header_bcp_gid,0);

  -- get file template_gid
  select
    filetemplate_gid into v_filetemplate_gid
  from recon_trn_tfile
  where file_gid = in_file_gid
  and delete_flag = 'N';

  set v_filetemplate_gid = ifnull(v_filetemplate_gid,0);

  select group_concat(col1) into v_tran_gid_all
       from recon_trn_tbcp
       where file_gid=in_file_gid
       and delete_flag='N' ;

  select group_concat(bcp_gid) into v_bcp_gid_all from recon_trn_tbcp
       where file_gid=in_file_gid
       and delete_flag='N';


  SET @inputString = v_tran_gid_all;
  SET @totalItems = LENGTH(@inputString) - LENGTH(REPLACE(@inputString, ',', '')) + 1;
  set v_tran_gid_all= SUBSTRING(@inputString, LOCATE(',', @inputString) + 1);
  set v_totaltran_count=@totalItems;

  set @inputString=''; set @totalItems='';

  SET @inputString = v_col_all;
  SET @totalItems = LENGTH(@inputString) - LENGTH(REPLACE(@inputString, ',', '')) + 1;
  set v_col_all= SUBSTRING(@inputString, LOCATE(',', @inputString) + 1);
  set v_totalcol_count=@totalItems;

  set @inputString=''; set @totalItems='';
  SET @inputString = in_tran_fieldall;
  set in_tran_fieldall= SUBSTRING(@inputString, LOCATE(',', @inputString) + 1);

  set  @inputString = v_bcp_gid_all;
  set v_bcp_gid_all= SUBSTRING(@inputString, LOCATE(',', @inputString) + 1);




  fieldvalue_validation:loop
    if j=v_totalcol_count-1 then
      leave fieldvalue_validation;
    end if;

    set j=j+1;
    set v_col_field= fn_get_splitstr(v_col_all,',',j);

    set v_sql = concat('select ',v_col_field,' into @col_value from recon_trn_tbcp ');
    set v_sql = concat(v_sql,'where file_gid = ',cast(in_file_gid as nchar),' ');
    set v_sql = concat(v_sql,'and bcp_gid = ',cast(v_header_bcp_gid as nchar),' ');
    set v_sql = concat(v_sql,'and delete_flag = ',char(39),'N',char(39));

    set @v_sql = v_sql;
    prepare _sql from @v_sql;
    execute _sql;
    deallocate prepare _sql;

    -- get mandatory flag
    set v_sql = concat('select mandatory_field into @mandatory_field from recon_mst_tfiletemplatefield ');
    set v_sql = concat(v_sql,'where filetemplate_gid = ',cast(v_filetemplate_gid as nchar),' ');
    set v_sql = concat(v_sql,'and excel_field = ', char(39), @col_value, char(39),' ');
    set v_sql = concat(v_sql,'and delete_flag = ',char(39),'N',char(39),' limit 1');

    set @v_sql = v_sql;
    prepare _sql from @v_sql;
    execute _sql;
    deallocate prepare _sql;

    if @mandatory_field = 'Y' then
      set v_sql = concat('select count(*) into @v_count from recon_trn_tbcp ');
      set v_sql = concat(v_sql,'where file_gid = ',cast(in_file_gid as nchar),' ');
      set v_sql = concat(v_sql,'and bcp_gid <> ',cast(v_header_bcp_gid as nchar),' ');
      set v_sql = concat(v_sql,'and ',v_col_field,' = ',char(39),char(39),' ');
      set v_sql = concat(v_sql,'and delete_flag = ',char(39),'N',char(39));

      set @v_sql = v_sql;
      prepare _sql from @v_sql;
      execute _sql;
      deallocate prepare _sql;

      set @v_count = ifnull(@v_count,0);

      if @v_count > 0 then
        set out_msg1='Blank or null values in Excel File';
        set out_result1=1;
        set err_flag=true;
        leave me;
      end if;
    end if;

	end loop fieldvalue_validation;

  set j=0;

  if err_flag=false then
    set i=0;
    set j=0;

    delete from recon_trn_ttran_updvalue where file_gid = in_file_gid;

    field_loop:loop
      if i=v_totaltran_count-1 then
        leave  field_loop;
      end if;

      set i=i+1;
      set v_tran_gid = fn_get_splitstr(v_tran_gid_all,',',i);
      set v_bcp_gid= fn_get_splitstr(v_bcp_gid_all,',',i);

    fieldvalue_loop:loop
      if j=v_totalcol_count-1 then
        leave fieldvalue_loop;
      end if;

      set j=j+1;
		  set v_tran_field= fn_get_splitstr(in_tran_fieldall,',',j);
	    set v_col_field= fn_get_splitstr(v_col_all,',',j);

        call pr_get_datatypecol(v_tran_field,in_file_gid,@out_colfield,@out_fieldtype);
        set v_field_type=@out_fieldtype;

	    if in_file_type='Q' then
        if v_field_type='DATE' then
           call pr_get_datevalue(v_col_field,v_bcp_gid,@out_result);
           set v_dateval=@out_result;

           set v_sql = concat('insert into recon_trn_ttranbrkup_updvalue (tranbrkup_gid,file_gid,field_name,field_value)
             values (',v_tran_gid,',',v_file_gid,',',char(39),v_tran_field,char(39),',',char(39),v_dateval,char(39),')');
		    else
          set v_sql = concat('insert into recon_trn_ttranbrkup_updvalue (tranbrkup_gid,file_gid,field_name,field_value)
						values (',v_tran_gid,',',v_file_gid,',',char(39),v_tran_field,char(39),',','(select ',v_col_field,' from recon_trn_tbcp where bcp_gid=',v_bcp_gid,'))');
		    end if;
		  else
        if v_field_type='DATE' then
          call pr_get_datevalue(v_col_field,v_bcp_gid,@out_result);

          set v_dateval=@out_result;

          set v_sql = concat('insert into recon_trn_ttran_updvalue (tran_gid,file_gid,field_name,field_value)
              values (',v_tran_gid,',',v_file_gid,',',char(39),v_tran_field,char(39),',',char(39),v_dateval,char(39),')');
        else
          set v_sql = concat('insert into recon_trn_ttran_updvalue (tran_gid,file_gid,field_name,field_value)
							values (',v_tran_gid,',',v_file_gid,',',char(39),v_tran_field,char(39),',','(select ',v_col_field,' from recon_trn_tbcp where bcp_gid=',v_bcp_gid,'))');
      end if;
    end if;

    set @v_sql = v_sql;
    prepare _sql from @v_sql;
    execute _sql;
    deallocate prepare _sql;
  end loop fieldvalue_loop;
  set j=0;
 end loop field_loop;
end if;

    call pr_upd_trantable(in_file_gid,in_file_type,@out_msg,@out_result);
    set out_msg1=@out_msg;
    set out_result1=@out_result;

END $$

DELIMITER ;