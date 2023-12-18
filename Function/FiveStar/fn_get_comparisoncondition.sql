DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_comparisoncondition` $$
CREATE FUNCTION `fn_get_comparisoncondition`(
  in_source_field text,
  in_target_field text,
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

  set v_org_target_field = in_target_field;

  
  set in_target_field = fn_get_filterformat(in_target_field,in_comparison_filter);

  if in_comparison_criteria = 'EXACT' then
    set v_txt = concat(' ',in_source_field,' = ',in_target_field,' ');
  elseif in_comparison_criteria = 'CONTAINS'  then
    set v_txt = concat(' ',in_target_field,' LIKE concat(''%'',',in_source_field,' collate ',v_collation,',''%'') ');
  elseif in_comparison_criteria = 'CONTAINS IN BASE'  then
    set v_txt = concat(' ',in_source_field,' LIKE concat(''%'',',in_target_field,' collate ',v_collation,',''%'') ');
  elseif in_comparison_criteria = 'BEGINS WITH'  then
    set v_txt = concat(' ',in_target_field,' LIKE concat(',in_source_field,' collate ',v_collation,',''%'') ');
  elseif in_comparison_criteria = 'ENDS WITH'  then
    set v_txt = concat(' ',in_target_field,' LIKE concat(''%'',',in_source_field,' collate ',v_collation,') ');
  elseif in_comparison_criteria = 'NOT CONTAINS'  then
    set v_txt = concat(' ',in_target_field,' NOT LIKE concat(''%'',',in_source_field,' collate ',v_collation,',''%'') ');
  elseif in_comparison_criteria = 'NOT CONTAINS IN BASE'  then
    set v_txt = concat(' ',in_source_field,' NOT LIKE concat(''%'',',in_target_field,' collate ',v_collation,',''%'') ');
  elseif substr(in_comparison_criteria,1,7) = 'BETWEEN' then
    set v_field_type = fn_get_fieldtype(in_target_field);
    set v_field_org_type = fn_get_fieldorgtype(in_target_field);

    set v_from = SPLIT(in_comparison_criteria,',',1);
    set v_to = SPLIT(in_comparison_criteria,',',2);

    set v_from = replace(v_from,'BETWEEN(','');
    set v_to = replace(v_to,')','');

    if v_field_type <> v_field_org_type then
      if v_field_org_type = 'TEXT' then
        if v_field_type = 'NUMBER' then
          set in_target_field = concat('cast(',in_target_field,' as decimal(15,2))');
        elseif v_field_type = 'DATE' then
          set in_target_field = concat('cast(',in_target_field,' as date)');
        end if;
      end if;
    end if;

    if v_field_type = 'DATE' then
      if v_field_org_type = 'DATE' then
        set v_txt = concat(' ',in_target_field,' >= adddate(',in_source_field,',',v_from,') and ',in_target_field,' <= adddate(',in_source_field,',',v_to,') ');
      else
        set v_txt = concat(' cast(',in_target_field,' as date) >= adddate(cast(',in_source_field,' as date),',v_from,') and cast(',in_target_field,' as date) <= adddate(cast(',in_source_field,' as date),',v_to,') ');
      end if;

      -- set v_txt = concat(' ',in_target_field,' >= adddate(',in_source_field,',',v_from,') and ',in_target_field,' <= adddate(',in_source_field,',',v_to,') ');
    elseif v_field_type = 'NUMBER' then
      set v_txt = concat(' ',in_target_field,' between (',in_source_field,'+',v_from,') and (',in_source_field,'+',v_to,') ');
    else
      set v_from = replace(v_from,'$FIELD$',in_target_field);
      set v_to = replace(v_to,'$FIELD$',in_target_field);

      set v_txt = concat(' ',in_source_field,' between (',v_from,') and (',v_to,') ');
    end if;
  elseif substr(in_comparison_criteria,1,11) = 'FIND_IN_SET' then
    set v_txt = trim(in_comparison_criteria);
    set in_target_field = v_org_target_field;
    set in_target_field = replace(v_txt,'$FIELD$',in_target_field);
    set in_target_field = fn_get_filterformat(in_target_field,in_comparison_filter);
    set in_target_field = substr(in_target_field,1,length(in_target_field)-1);

    set v_txt = concat(' ',in_target_field,',',in_source_field,') > 0 ');
  elseif instr(in_comparison_criteria,'$FIELD$') > 0 then
    set v_txt = trim(in_comparison_criteria);
    set in_target_field = v_org_target_field;
    set in_target_field = replace(v_txt,'$FIELD$',in_target_field);
    set in_target_field = fn_get_filterformat(in_target_field,in_comparison_filter);

    set v_txt = concat(' ',in_source_field,' = ',in_target_field,' ');
  else
    set in_target_field = v_org_target_field;
    set in_target_field = fn_get_filterformat(in_target_field,in_comparison_filter);
    set v_txt = concat(' ',in_source_field,' ',in_comparison_criteria,' ',in_target_field,' ');
  end if;

  return v_txt;
end $$

DELIMITER ;