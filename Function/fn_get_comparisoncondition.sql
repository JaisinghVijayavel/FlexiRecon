DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_comparisoncondition` $$
CREATE FUNCTION `fn_get_comparisoncondition`(
  in_recon_code text,
  in_source_field text,
  in_comparison_field text,
  in_comparison_criteria text,
  in_comparison_filter int
) RETURNS text CHARSET latin1
begin
  declare v_txt text;
  declare v_field_type text;
  declare v_field_org_type text;
  declare v_from text;
  declare v_to text;
  declare v_org_target_field text;
  declare v_collation text;

  select @@collation_database into v_collation;

  set v_org_target_field = in_comparison_field;


  if in_comparison_criteria = 'EXACT' then
    set v_txt = concat(' ',in_source_field,' = ',in_comparison_field,' ');
  elseif in_comparison_criteria = 'CONTAINS'  then
    set v_txt = concat(' ',in_comparison_field,' LIKE concat(''%'',',in_source_field,' collate ',v_collation,',''%'') ');
  elseif in_comparison_criteria = 'CONTAINS IN BASE'  then
    set v_txt = concat(' ',in_source_field,' LIKE concat(''%'',',in_comparison_field,' collate ',v_collation,',''%'') ');
  elseif in_comparison_criteria = 'BEGINS WITH'  then
    set v_txt = concat(' ',in_comparison_field,' LIKE concat(',in_source_field,' collate ',v_collation,',''%'') ');
  elseif in_comparison_criteria = 'NOT BEGINS WITH'  then
    set v_txt = concat(' ',in_comparison_field,' NOT LIKE concat(',in_source_field,' collate ',v_collation,',''%'') ');
  elseif in_comparison_criteria = 'ENDS WITH'  then
    set v_txt = concat(' ',in_comparison_field,' LIKE concat(''%'',',in_source_field,' collate ',v_collation,') ');
  elseif in_comparison_criteria = 'NOT ENDS WITH'  then
    set v_txt = concat(' ',in_comparison_field,' NOT LIKE concat(''%'',',in_source_field,' collate ',v_collation,') ');
  elseif in_comparison_criteria = 'NOT CONTAINS'  then
    set v_txt = concat(' ifnull(',in_comparison_field,','''') NOT LIKE concat(''%'',',in_source_field,' collate ',v_collation,',''%'') ');
  elseif in_comparison_criteria = 'NOT CONTAINS IN BASE'  then
    set v_txt = concat(' ',in_source_field,' NOT LIKE concat(''%'',',in_comparison_field,' collate ',v_collation,',''%'') ');
  elseif substr(in_comparison_criteria,1,7) = 'BETWEEN' then
    set v_field_type = fn_get_fieldtype(in_recon_code,in_comparison_field);
    set v_field_org_type = fn_get_fieldorgtype(in_recon_code,in_comparison_field);

    set v_from = SPLIT(in_comparison_criteria,',',1);
    set v_to = SPLIT(in_comparison_criteria,',',2);

    set v_from = replace(v_from,'BETWEEN(','');
    set v_to = replace(v_to,')','');

    if v_field_type <> v_field_org_type then
      if v_field_org_type = 'TEXT' then
        if v_field_type = 'NUMBER' or v_field_type = 'NUMERIC' then
          set in_comparison_field = concat('cast(',in_comparison_field,' as decimal(15,2))');
        elseif v_field_type = 'INTEGER' then
          set in_comparison_field = concat('cast(',in_comparison_field,' as signed)');
        elseif v_field_type = 'DATE' then
          set in_comparison_field = concat('cast(',in_comparison_field,' as date)');
        elseif v_field_type = 'DATETIME' then
          set in_comparison_field = concat('cast(',in_comparison_field,' as datetime)');
        end if;
      end if;
    end if;

    if v_field_type = 'DATE' or v_field_type = 'DATETIME' then
      -- set v_txt = concat(' ',in_comparison_field,' >= adddate(',in_source_field,',',v_from,') and ',in_comparison_field,' <= adddate(',in_source_field,',',v_to,') ');
      if v_field_org_type = 'DATE' then
        set v_txt = concat(' ',in_comparison_field,' >= adddate(',in_source_field,',',v_from,') and ',in_comparison_field,' <= adddate(',in_source_field,',',v_to,') ');
      elseif v_field_type = 'DATE' then
        set v_txt = concat(' cast(',in_comparison_field,' as date) >= adddate(cast(',in_source_field,' as date),',v_from,') and cast(',in_comparison_field,' as date) <= adddate(cast(',in_source_field,' as date),',v_to,') ');
      elseif v_field_type = 'DATETIME' then
        set v_txt = concat(' cast(',in_comparison_field,' as datetime) >= adddate(cast(',in_source_field,' as datetime),',v_from,') and cast(',in_comparison_field,' as datetime) <= adddate(cast(',in_source_field,' as datetime),',v_to,') ');
      end if;
    elseif v_field_type = 'NUMBER' or v_field_type = 'INTEGER' or v_field_type = 'NUMERIC' then
      set in_source_field = fn_get_fieldnamecast(in_recon_code,in_source_field);

      set v_txt = concat(' ',in_comparison_field,' between (',in_source_field,'+',v_from,') and (',in_source_field,'+',v_to,') ');
    else
      set v_from = replace(v_from,'$FIELD$',in_comparison_field);
      set v_to = replace(v_to,'$FIELD$',in_comparison_field);

      set in_source_field = fn_get_fieldnamecast(in_recon_code,in_source_field);
      set v_txt = concat(' ',in_source_field,' between (',v_from,') and (',v_to,') ');
    end if;
  elseif substr(in_comparison_criteria,1,11) = 'FIND_IN_SET' then
    set v_txt = trim(in_comparison_criteria);
    set in_comparison_field = v_org_target_field;
    set in_comparison_field = replace(v_txt,'$FIELD$',in_comparison_field);
    set in_comparison_field = substr(in_comparison_field,1,length(in_comparison_field)-1);

    set v_txt = concat(' ',in_comparison_field,',',in_source_field,') > 0 ');
  elseif instr(in_comparison_criteria,'$FIELD$') > 0
    and instr(in_comparison_criteria,'$COMPARISON_FIELD$') = 0 then
    set v_txt = trim(in_comparison_criteria);

    set in_comparison_field = v_org_target_field;
    set in_comparison_field = fn_get_fieldnamecast(in_recon_code,in_comparison_field);
    set in_comparison_field = replace(v_txt,'$FIELD$',in_comparison_field);

    set in_source_field = fn_get_fieldnamecast(in_recon_code,in_source_field);
    set v_txt = concat(' ',in_source_field,' = ',in_comparison_field,' ');
  elseif instr(in_comparison_criteria,'$COMPARISON_FIELD$') > 0 then
    -- comparison criteria condition type added with function
    set v_txt = trim(in_comparison_criteria);

    set in_comparison_field = v_org_target_field;
    set in_comparison_field = fn_get_fieldnamecast(in_recon_code,in_comparison_field);

    set in_comparison_field = replace(v_txt,'$COMPARISON_FIELD$',in_comparison_field);

    set in_source_field = fn_get_fieldnamecast(in_recon_code,in_source_field);

    if instr(in_comparison_criteria,'$SOURCE_FIELD$') > 0 then
      set v_txt = replace(in_comparison_field,'$SOURCE_FIELD$',in_source_field);
    else
      set v_txt = concat(' ',in_source_field,' ',in_comparison_field,' ');
    end if;
  else
    set in_comparison_field = v_org_target_field;
    set in_comparison_field = fn_get_fieldnamecast(in_recon_code,in_comparison_field);
    set in_source_field = fn_get_fieldnamecast(in_recon_code,in_source_field);

    set v_txt = concat(' ',in_comparison_field,' ',in_comparison_criteria,' ',in_source_field,' ');
  end if;

  return v_txt;
end $$

DELIMITER ;