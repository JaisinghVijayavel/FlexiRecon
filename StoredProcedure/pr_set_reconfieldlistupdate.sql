DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_reconfieldlistupdate` $$
CREATE PROCEDURE `pr_set_reconfieldlistupdate`(
  in in_recon_code varchar(32),
  in in_tran_gid int,
  in in_tranbrkp_gid int,
  in in_curr_value json,
  in in_new_value json,
  in in_user_code varchar(255),
  out out_msg text,
  out out_result int(10)
)
me:BEGIN
  /*
    Created By : Vijayavel
    Created Date: 08-12-2025

    Updated By : Vijayavel
    updated Date :

	  Version - 1
  */

  declare v_concurrent_ko_flag text default '';

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';

	declare v_field_name text default '';
	declare v_field_value text default '';

	declare v_sql text default '';

  set out_result = 0;
  set out_msg = 'Failed';

  drop temporary table if exists recon_tmp_t6pseudorows;
  drop temporary table if exists recon_tmp_tfieldvalue;

  CREATE temporary TABLE recon_tmp_t6pseudorows(
    row int unsigned NOT NULL,
    PRIMARY KEY (row)
  ) ENGINE = MyISAM;

	CREATE TEMPORARY TABLE recon_tmp_tfieldvalue
	(
		fieldvalue_gid int unsigned not null AUTO_INCREMENT,
		tran_gid int not null default 0,
		tranbrkp_gid int not null default 0,
		field_name varchar(255) default null,
		field_value text default null,
		PRIMARY KEY (fieldvalue_gid),
		key idx_field_name(field_name)
	) ENGINE = MyISAM;

  insert into recon_tmp_t6pseudorows select row from pseudo_rows1 where row <= 255;

  insert into recon_tmp_tfieldvalue (tran_gid,tranbrkp_gid,field_name,field_value)
	select
		in_tran_gid,
		in_tranbrkp_gid,
		JSON_UNQUOTE(JSON_EXTRACT(a.new_json_value, CONCAT('$[', recon_tmp_t6pseudorows.row, '].field_name'))) AS field_name,
		JSON_UNQUOTE(JSON_EXTRACT(a.new_json_value, CONCAT('$[', recon_tmp_t6pseudorows.row, '].field_value'))) AS field_value
	FROM (select in_new_value as new_json_value) as a
	JOIN recon_tmp_t6pseudorows
	HAVING field_name IS NOT NULL;

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

	-- reconfield block
	reconfield_block:begin
		declare reconfield_done int default 0;
		declare reconfield_cursor cursor for
			select field_name,field_value from recon_tmp_tfieldvalue;
		declare continue handler for not found set reconfield_done=1;

		open reconfield_cursor;

		reconfield_loop: loop
			fetch reconfield_cursor into v_field_name,v_field_value;
			if reconfield_done = 1 then leave reconfield_loop; end if;

			set v_field_name = ifnull(v_field_name,'');
			set v_field_value = ifnull(v_field_value,'');

			-- set new value
			if in_tranbrkp_gid > 0 then
				set v_sql = concat("update ",v_tranbrkp_table," set ",
					v_field_name," = '",v_field_value,"'
					where tranbrkp_gid = ",cast(in_tranbrkp_gid as nchar),"
					and delete_flag = 'N'");
			else
				set v_sql = concat("update ",v_tran_table," set ",
					v_field_name," = '",v_field_value,"'
					where tran_gid = ",cast(in_tran_gid as nchar),"
					and delete_flag = 'N'");
			end if;

			set @v_sql4 = v_sql;
			prepare sql14_stmt from @v_sql4;
			execute sql14_stmt;
			deallocate prepare sql14_stmt;
		end loop reconfield_loop;

		close reconfield_cursor;
	end reconfield_block;

  -- insert into manual update log
  insert into recon_trn_tfieldupdatemanual
  (
         recon_code,
         tran_gid,
         tranbrkp_gid,
         new_value,
         old_value,
         update_date,
         update_by
  )
  select in_recon_code,
         in_tran_gid,
         in_tranbrkp_gid,
         in_new_value,
         in_curr_value,
         sysdate(),
         in_user_code;

  drop temporary table if exists recon_tmp_tfieldvalue;
  drop temporary table if exists recon_tmp_t6pseudorows;

  set out_result = 1;
  set out_msg = 'Record updated successfully !';
END $$

DELIMITER ;