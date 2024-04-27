DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_datevalue` $$
CREATE PROCEDURE `pr_get_datevalue`(
  in in_col_field text,
  in in_bcp_gid int ,
  out out_result text)
BEGIN
   
  declare v_sql text default '';
  declare v_date_format text default '';

  select fn_get_configvalue('iis_date_format') into v_date_format;

  set v_date_format = ifnull(v_date_format,'');

  if v_date_format = '' then
    set v_date_format = '%d-%m-%Y %H:%i:%s';
  end if;

   set v_sql=concat('select date_format(str_to_date(', in_col_field, ',''%d-%m-%Y'')',',''%Y-%m-%d'')
                   into @v_date_value from recon_trn_tbcp where bcp_gid = ', in_bcp_gid,'');

  set @v_sql = v_sql;

 
  prepare _sql from @v_sql;
  execute _sql;
  deallocate prepare _sql;

  if @v_date_value = '0000-00-00' then
    set @v_date_value = '';
  end if;

  set out_result=@v_date_value;
END $$

DELIMITER ;