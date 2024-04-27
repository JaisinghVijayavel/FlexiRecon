DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_datatypecol` $$
CREATE PROCEDURE `pr_get_datatypecol`(
  in in_tran_field text,
  in in_file_gid int ,
  out out_colfield text,
  out out_fieldtype text 
 )
BEGIN
  declare v_tranfield text default '';
  declare v_field_type text default '';

  select
    a.col_field_name ,b.field_type
  into
    v_tranfield,v_field_type
  from recon_trn_tbcp_upd a
  inner join  recon_mst_tfieldstru b on a.col_field_name=b.field_name
  where a.file_gid=in_file_gid
  and a.col_field_name=in_tran_field
  and a.delete_flag='N';

  set out_colfield=v_tranfield;
  set out_fieldtype=v_field_type;
END $$

DELIMITER ;