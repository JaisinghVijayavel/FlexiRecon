DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_autocode`$$
CREATE FUNCTION `fn_get_autocode`(in_activity_code varchar(8)) RETURNS text
begin
  declare v_autocode_gid int;
  declare v_auto_code text;
  declare v_code_prefix text;

  declare v_rec_slno int;
  declare v_len_leading_zero int;

  select
    autocode_gid,
    rec_slno,
    code_prefix,
    len_leading_zero
  into
    v_autocode_gid,
    v_rec_slno,
    v_code_prefix,
    v_len_leading_zero
  from admin_mst_tautocode
  where activity_code = in_activity_code
  and active_status = 'Y'
  and delete_flag = 'N';

  set v_autocode_gid = ifnull(v_autocode_gid,0);
  set v_rec_slno = ifnull(v_rec_slno,0);
  set v_code_prefix = ifnull(v_code_prefix,'');
  set v_len_leading_zero = ifnull(v_len_leading_zero,0);

  if v_len_leading_zero < length(cast(v_rec_slno as char)) then
    set v_len_leading_zero = length(cast(v_rec_slno as char));
  end if;

  set v_auto_code = concat(v_code_prefix,lpad(v_rec_slno,v_len_leading_zero,'0'));

  update admin_mst_tautocode set
    rec_slno = rec_slno + 1
  where autocode_gid = v_autocode_gid
  and delete_flag = 'N';

  return v_auto_code;
end;

 $$

DELIMITER ;