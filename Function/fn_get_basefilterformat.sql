DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_basefilterformat` $$
CREATE FUNCTION `fn_get_basefilterformat`(
  in_filter_field varchar(128),
  in_filter_criteria text,
  in_add_filter int,
  in_comparison_criteria text,
  in_ident_value_flag text,
  in_ident_value text
) RETURNS text
begin
  declare v_filter_field text default '';
  declare v_txt text default '';
  declare v_collation text default '';

  select @@collation_database into v_collation;

  if in_ident_value_flag = 'Y' then
    -- get $FIELD$ value
    set in_ident_value = fn_get_filtervalue('',in_ident_value,'');
  end if;

	-- filter criteria
  set v_filter_field = trim(in_filter_criteria);

  set v_filter_field = replace(v_filter_field,'$FIELD$',in_filter_field);

  if upper(v_filter_field) = 'EXACT' then
    set v_filter_field = in_filter_field;
  end if;


  set v_filter_field = fn_get_filterformat(v_filter_field,in_add_filter);

  -- comparison criteria =
  set in_comparison_criteria = trim(in_comparison_criteria);

  -- replace $IDENTIFIER_FIELD$ with $FILTER_FIELD$
  set in_comparison_criteria = replace(in_comparison_criteria,'$SOURCE_FIELD$','$FIELD$');
  set in_comparison_criteria = replace(in_comparison_criteria,'$COMPARISON_FIELD$','$FILTER_FIELD$');
  set in_comparison_criteria = replace(in_comparison_criteria,'$IDENT_FIELD$','$FILTER_FIELD$');

  if trim(in_comparison_criteria) = '=' then
    set in_comparison_criteria = 'EXACT';
  end if;

  if in_comparison_criteria = 'EXACT' then
		if in_ident_value_flag = 'Y' then
			if lower(trim(in_ident_value)) = 'null' then
				set v_txt = concat(v_filter_field, ' is null ');
			else
				set v_txt = concat(v_filter_field,' = ',char(39),in_ident_value,char(39),' ');
			end if;
		else
				set v_txt = concat(v_filter_field,' = ',in_ident_value,' ');
		end if;
  elseif in_comparison_criteria = 'CONTAINS'  then
		if in_ident_value_flag = 'Y' then
			set v_txt = concat(v_filter_field,' LIKE ',char(39),'%',in_ident_value,'%',char(39),' ');
		else
			set v_txt = concat(' ',v_filter_field,' LIKE concat(''%'',',in_ident_value,' collate ',v_collation,',''%'') ');
		end if;
  elseif in_comparison_criteria = 'BEGINS WITH'  then
		if in_ident_value_flag = 'Y' then
			set v_txt = concat(v_filter_field,' LIKE ',char(39),in_ident_value,'%',char(39),' ');
		else
			set v_txt = concat(' ',v_filter_field,' LIKE concat(',in_ident_value,' collate ',v_collation,',''%'') ');
		end if;
  elseif in_comparison_criteria = 'NOT BEGINS WITH'  then
		if in_ident_value_flag = 'Y' then
			set v_txt = concat(v_filter_field,' NOT LIKE ',char(39),in_ident_value,'%',char(39),' ');
		else
			set v_txt = concat(' ',v_filter_field,' NOT LIKE concat(',in_ident_value,' collate ',v_collation,',''%'') ');
		end if;
  elseif in_comparison_criteria = 'ENDS WITH'  then
		if in_ident_value_flag = 'Y' then
			set v_txt = concat(v_filter_field,' LIKE ',char(39),'%',in_ident_value,char(39),' ');
		else
			set v_txt = concat(' ',v_filter_field,' LIKE concat(''%'',',in_ident_value,' collate ',v_collation,') ');
		end if;
  elseif in_comparison_criteria = 'NOT ENDS WITH'  then
		if in_ident_value_flag = 'Y' then
			set v_txt = concat(v_filter_field,' NOT LIKE ',char(39),'%',in_ident_value,char(39),' ');
		else
			set v_txt = concat(' ',v_filter_field,' NOT LIKE concat(''%'',',in_ident_value,' collate ',v_collation,') ');
		end if;
  elseif in_comparison_criteria = 'NOT CONTAINS'  then
		if in_ident_value_flag = 'Y' then
			set v_txt = concat('ifnull(',v_filter_field,','''') NOT LIKE ',char(39),'%',in_ident_value,'%',char(39),' ');
		else
			set v_txt = concat(' ',v_filter_field,' NOT LIKE concat(''%'',',in_ident_value,' collate ',v_collation,',''%'') ');
		end if;
  elseif instr(in_comparison_criteria,'$FIELD$') > 0 or instr(in_comparison_criteria,'$FILTER_FIELD$') > 0 then
    if instr(in_comparison_criteria,'$FIELD$') > 0 then
			set v_filter_field = replace(in_comparison_criteria,'$FIELD$',v_filter_field);
    else
      set v_filter_field = concat(v_filter_field,' ',in_comparison_criteria);
    end if;

    if instr(in_comparison_criteria,'$FILTER_FIELD$') > 0 then
			set v_txt = replace(v_filter_field,'$FILTER_FIELD$',in_ident_value);
    elseif upper(in_ident_value) <> '$ADHOC$' then
		  if in_ident_value_flag = 'Y' then
			  set v_txt = concat(v_filter_field,' = ',char(39),in_ident_value,char(39),' ');
		  else
			  set v_txt = concat(v_filter_field,' = ',in_ident_value,' ');
		  end if;
    else
      set v_txt = v_filter_field;
    end if;
  elseif in_comparison_criteria = '<>' then
		if in_ident_value_flag = 'Y' then
			if lower(trim(in_ident_value)) = 'null' then
				set v_txt = concat(v_filter_field, ' is not null ');
			else
				set v_txt = concat(v_filter_field,' <> ',char(39),in_ident_value,char(39),' ');
			end if;
		else
			set v_txt = concat(v_filter_field,' <> ',in_ident_value,' ');
		end if;
  elseif in_ident_value = '$ADHOC$' then
    set v_txt = concat(v_filter_field,' ',in_comparison_criteria,' ');
  else
		if in_ident_value_flag = 'Y' then
			set v_txt = concat(v_filter_field,' ',in_comparison_criteria,' ',char(39),in_ident_value,char(39),' ');
		else
			set v_txt = concat(v_filter_field,' ',in_comparison_criteria,' ',in_ident_value,' ');
    end if;
  end if;

  if trim(v_txt) = '' then
    set v_txt = ' 1 = 1 ';
  end if;

  return v_txt;
end $$

DELIMITER ;