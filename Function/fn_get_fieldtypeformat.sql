DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_fieldtypeformat` $$
CREATE FUNCTION `fn_get_fieldtypeformat`
(
  in_recon_code varchar(32),
  in_field_name varchar(128),
  in_field_type varchar(128),
  in_field_length varchar(128)
) RETURNS text
begin
  declare v_field_type varchar(128) default '';
  declare v_field_format varchar(128) default '';
  declare v_field_formatted varchar(128) default '';
  declare v_field_length varchar(128) default '';
  declare v_txt text;
  declare n int default 0;

  set v_field_type = ifnull(in_field_type,'TEXT');
  set v_field_format = ifnull(in_field_length,'');

  if v_field_type = 'INTEGER' then
    set v_txt = concat('ifnull(cast(',in_field_name,' as signed),0)');
  elseif v_field_type = 'NUMERIC' then
    if v_field_format = '' then
      set v_field_format = '15,2';
    end if;

    set v_txt = concat('ifnull(cast(',in_field_name,' as decimal(',v_field_format,')),0)');
  elseif v_field_type = 'DATE' then
    set v_field_format = ifnull(fn_get_configvalue('app_date_format'),'');

    if v_field_format <> '' then
      set v_txt = concat('date_format(',in_field_name,',',char(39),v_field_format,char(39),')');
    else
      set v_txt = in_field_name;
    end if;
  elseif v_field_type = 'DATETIME' then
    set v_field_format = ifnull(fn_get_configvalue('app_datetime_format'),'');

    if v_field_format <> '' then
      set v_txt = concat('date_format(',in_field_name,',',char(39),v_field_format,char(39),')');
    else
      set v_txt = in_field_name;
    end if;
  else
    set v_txt = concat('ifnull(cast(',in_field_name,' as nchar),',char(39),char(39),')');
  end if;

  return v_txt;
end $$

DELIMITER ;