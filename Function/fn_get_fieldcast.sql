DELIMITER $$

DROP function IF EXISTS `fn_get_fieldcast` $$
CREATE function `fn_get_fieldcast`
(
  in_recon_code varchar(32),
  in_field_desc varchar(255)
) returns text
begin
  declare v_field_name varchar(255);
  declare v_field_type varchar(128);

	select
		recon_field_name,
		recon_field_type
	into
		v_field_name,
		v_field_type
	from recon_mst_treconfield
	where recon_field_desc = in_field_desc
	and recon_code = in_recon_code
	and delete_flag = 'N';

	set v_field_name = ifnull(v_field_name,'');
	set v_field_type = ifnull(v_field_type,'');

  if v_field_name  = '' then
	  select
		  field_name,
		  field_type
	  into
		  v_field_name,
		  v_field_type
	  from recon_mst_tfieldstru
	  where field_alias_name = in_field_desc
	  and delete_flag = 'N';

	  set v_field_name = ifnull(v_field_name,'');
	  set v_field_type = ifnull(v_field_type,'');
  end if;

  if v_field_type = '' then
    set v_field_type = 'TEXT';
  end if;

  if v_field_type = 'INTEGER' then
    set v_field_name = concat("cast(",v_field_name," as unsigned)");
  elseif v_field_type = 'NUMERIC' then
    set v_field_name = concat("cast(",v_field_name," as decimal(15,3))");
  elseif v_field_type = 'DATE' then
    set v_field_name = concat("cast(",v_field_name," as date)");
  elseif v_field_type = 'DATRETIME' then
    set v_field_name = concat("cast(",v_field_name," as datetime)");
  end if;

  return v_field_name;
end $$

DELIMITER ;