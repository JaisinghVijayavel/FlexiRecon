DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_fieldformat` $$
CREATE FUNCTION `fn_get_fieldformat`
(
  in_recon_code varchar(32),
  in_field_name varchar(128)
) RETURNS varchar(255)
begin
  declare v_field_type varchar(128) default '';
  declare v_field_format varchar(128) default '';
  declare v_field_formatted varchar(128) default '';
  declare v_field_length varchar(128) default '';
  declare v_txt text;
  declare n int default 0;

  if not exists(select recon_code from recon_mst_treconfield
    where recon_field_name = in_field_name
    and recon_code = in_recon_code
    and delete_flag = 'N') then
    return in_field_name;
  end if;

  select recon_field_length into v_field_format from recon_mst_treconfield
  where recon_field_name = in_field_name
  and recon_code = in_recon_code
  and delete_flag = 'N' limit 0,1;

  set v_field_type = v_field_format;
  set v_field_type = ifnull(v_field_type,'');

  set n = instr(v_field_type,'(');

  if n > 1 then
    set v_txt = v_field_type;
    set v_field_type = substr(v_txt,1,n-1);
    set v_txt = substr(v_txt,n+1);

    set n = instr(v_txt,')');
    if n > 1 then
      set v_field_format = substr(v_txt,1,n-1);
    end if;
  end if;

  if v_field_type = 'EXACT' or v_field_type = '' then
    return in_field_name;
  elseif v_field_type = 'SUBSTR' and v_field_format <> '' then
    set v_txt = concat('substr(',in_field_name,',',v_field_format,')');
    return v_txt;
  elseif v_field_type = 'ROUND' and v_field_format <> '' then
    set v_txt = concat('round(',in_field_name,',',v_field_format,')');
    return v_txt;
  elseif v_field_type = 'DATE' and v_field_format <> '' then
    set v_field_format = ifnull(fn_get_configvalue('app_date_format'),'');

    if v_field_format <> '' then
      set v_txt = concat('date_format(',in_field_name,',',char(39),v_field_format,char(39),')');
    else
      set v_txt = in_field_name;
    end if;
    return v_txt;
  elseif v_field_type = 'EXACTMULT' then
    set v_txt = concat(in_field_name,'*mult');
    return v_txt;
  else
    return in_field_name;
  end if;
end $$

DELIMITER ;