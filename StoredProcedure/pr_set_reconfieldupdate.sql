DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_reconfieldupdate` $$
CREATE PROCEDURE `pr_set_reconfieldupdate`(
  in in_recon_code varchar(32),
  in in_tran_gid int,
  in in_tranbrkp_gid int,
  in in_recon_field_name varchar(32),
  in in_recon_field_value text,
  in in_user_code varchar(255),
  out out_msg text,
  out out_result int(10)
)
BEGIN
  /*
    Created By : Vijayavel
    Created Date: 03-12-2025

    Updated By : Vijayavel
    updated Date :

	  Version - 1
  */

  declare v_concurrent_ko_flag text default '';

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';

	declare v_old_field_value text default '';
	declare v_sql text default '';

  set out_result = 0;
  set out_msg = 'Failed';

  -- concurrent KO flag
  set v_concurrent_ko_flag = fn_get_configvalue('concurrent_ko_flag');

  if v_concurrent_ko_flag = 'Y' then
		if in_tranbrkp_gid > 0 then
			set v_tranbrkp_table = concat(in_recon_code,'_tranbrkp');
		else
			set v_tran_table = concat(in_recon_code,'_tran');
		end if;
  else
		if in_tranbrkp_gid > 0 then
			set v_tranbrkp_table = 'recon_trn_ttranbrkp';
		else
			set v_tran_table = 'recon_trn_ttran';
		end if;
  end if;

  -- get old value
	set @v_old_field_value  = '';

	if in_tranbrkp_gid > 0 then
		set v_sql = concat("select cast(",in_recon_field_name," as nchar) into @v_old_field_value
			from ",v_tranbrkp_table,"
			where tranbrkp_gid = ",cast(in_tranbrkp_gid as nchar),"
			and delete_flag = 'N'");
	else
		set v_sql = concat("select cast(",in_recon_field_name," as nchar) into @v_old_field_value
			from ",v_tran_table,"
			where tran_gid = ",cast(in_tran_gid as nchar),"
			and delete_flag = 'N'");
	end if;

	set @v_sql3 = v_sql;
	prepare sql13_stmt from @v_sql3;
	execute sql13_stmt;
	deallocate prepare sql13_stmt;

	set v_old_field_value = @v_old_field_value;
	set v_old_field_value = ifnull(v_old_field_value,'');

  -- set new value
	if in_tranbrkp_gid > 0 then
		set v_sql = concat("update ",v_tranbrkp_table," set ",
      in_recon_field_name," = '",in_recon_field_value,"'
			where tranbrkp_gid = ",cast(in_tranbrkp_gid as nchar),"
			and delete_flag = 'N'");
	else
		set v_sql = concat("update ",v_tran_table," set ",
      in_recon_field_name," = '",in_recon_field_value,"'
			where tran_gid = ",cast(in_tran_gid as nchar),"
			and delete_flag = 'N'");
	end if;

	set @v_sql4 = v_sql;
	prepare sql14_stmt from @v_sql4;
	execute sql14_stmt;
	deallocate prepare sql14_stmt;

  -- insert into manual update log
  insert into recon_trn_tfieldupdatemanual
  (
         recon_code,
         tran_gid,
         tranbrkp_gid,
         recon_field_name,
         new_value,
         old_value,
         update_date,
         update_by
  )
  select in_recon_code,
         in_tran_gid,
         in_tranbrkp_gid,
         in_recon_field_name,
         in_recon_field_value,
         v_old_field_value,
         sysdate(),
         in_user_code;

  set out_result = 1;
  set out_msg = 'Record updated successfully !';
END $$

DELIMITER ;