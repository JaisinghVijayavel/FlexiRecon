DELIMITER $$

DROP function IF EXISTS `fn_get_reconfieldtype` $$
CREATE function `fn_get_reconfieldtype`
(
  in_recon_code varchar(32),
  in_field_name varchar(255)
) returns text
begin
  declare v_field_name varchar(255);
  declare v_field_type varchar(128);

  if instr(in_field_name,'.') = 0 then
	  select
		  recon_field_name,
		  recon_field_type
	  into
		  v_field_name,
		  v_field_type
	  from recon_mst_treconfield
	  where recon_field_name = in_field_name
	  and recon_code = in_recon_code
	  and delete_flag = 'N';
  else
	  select
		  recon_field_name,
		  recon_field_type
	  into
		  v_field_name,
		  v_field_type
	  from recon_mst_treconfield
	  where recon_field_name = split(in_field_name,'.',2)
	  and recon_code = in_recon_code
	  and delete_flag = 'N';
  end if;

	set v_field_name = ifnull(v_field_name,'');
	set v_field_type = ifnull(v_field_type,'TEXT');

  if v_field_name  = '' then
	  select
		  field_name,
		  field_type
	  into
		  v_field_name,
		  v_field_type
	  from recon_mst_tfieldstru
	  where field_name = in_field_name
	  and delete_flag = 'N';

	  set v_field_name = ifnull(v_field_name,'');
	  set v_field_type = ifnull(v_field_type,'');
  end if;

  if v_field_type = '' then
    set v_field_type = 'TEXT';
  end if;

  return v_field_type;
end $$

DELIMITER ;