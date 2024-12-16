DELIMITER $$
DROP PROCEDURE IF EXISTS `pr_set_fieldupdate` $$
CREATE procedure `pr_set_fieldupdate`
(
  in in_scheduler_gid int,
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32),
  out out_msg text,
  out out_result int
)
me:begin
  /*
    Created By : Vijayavel
    Created Date : 13-09-2024

    Updated By : Vijayavel
    Updated Date :

    Version : 1
  */
  declare v_tran_gid int default 0;
  declare v_tranbrkp_gid int default 0;
  declare v_dataset_gid int default 0;

  declare v_recon_code text default '';
  declare v_dataset_code text default '';
  declare v_dataset_db_name text default '';
  declare v_dataset_field_name text default '';
  declare v_dataset_field_type text default '';

  declare v_recon_field_desc text default '';
  declare v_dataset_field_desc text default '';

  declare v_field_desc text default '';
  declare v_field_value text default '';
  declare v_old_field_value text default '';

  declare v_recon_field text default '';
  declare v_recon_field_type text default '';

  declare v_tran_table text default '';
  declare v_tranbrkp_table text default '';

  declare v_sql text default '';

  -- set tran table
  set v_tran_table = 'recon_trn_ttran';
  set v_tranbrkp_table = 'recon_trn_ttranbrkp';

  set v_dataset_db_name = fn_get_configvalue('dataset_db_name');

  -- update field
  updatefield_block:begin
    declare updatefield_done int default 0;
    declare updatefield_cursor cursor for
      select
        tran_gid,tranbrkp_gid,dataset_gid,dataset_code,recon_code,field_desc,field_value
      from recon_trn_tfieldupdate
      where scheduler_gid = in_scheduler_gid
      and delete_flag = 'N';
    declare continue handler for not found set updatefield_done=1;

    open updatefield_cursor;

    updatefield_loop: loop
      fetch updatefield_cursor into v_tran_gid,v_tranbrkp_gid,v_dataset_gid,v_dataset_code,
                                    v_recon_code,v_field_desc,v_field_value;

      if updatefield_done = 1 then leave updatefield_loop; end if;

      set v_dataset_code = ifnull(v_dataset_code,'');
      set v_recon_code = ifnull(v_recon_code,'');

      set v_field_desc = ifnull(v_field_desc,'');
      set v_field_value = ifnull(v_field_value,'');

      -- get recon field and type
      -- call pr_get_fieldname(v_recon_code,v_recon_field_desc,v_recon_field,v_recon_field_type);

      if v_tran_gid > 0 or v_tranbrkp_gid > 0 then
        if v_recon_code = '' then
          if v_tran_gid > 0 then
            set v_sql = concat("select recon_code into @recon_code from ",v_tran_table,"
              where tran_gid = ",cast(v_tran_gid as nchar),"
              and delete_flag = 'N'");

            call pr_run_sql1(v_sql,@msg,@result);

            set v_recon_code = ifnull(@recon_code,'');
          end if;
        end if;

        set v_recon_field_desc = v_field_desc;
        set v_recon_field = fn_get_reconfieldfromdesc(v_recon_code,v_recon_field_desc);
      elseif v_dataset_gid > 0 and v_dataset_code <> '' then
        set v_dataset_field_desc = v_field_desc;

        -- get dataset field name and type
        call pr_get_dsfldinfofromdesc(v_dataset_code,v_dataset_field_desc,@dataset_field_name,@dataset_field_type);

        set v_dataset_field_name = @dataset_field_name;
        set v_dataset_field_type = @dataset_field_type;

        -- dataset table
        if v_dataset_db_name <> '' then
          set v_dataset_code = concat(v_dataset_db_name,'.',v_dataset_code);
        end if;
      else
        set v_recon_field = '';
        set v_dataset_field_name = '';
      end if;

      -- field value
			if trim(lower(v_field_value)) = 'blank' then
				set v_field_value = '';
			end if;

			if trim(lower(v_field_value)) = 'null' then
				set v_field_value = 'null';
			else
				set v_field_value = concat("'",v_field_value,"'");
			end if;

			set @old_field_value = '';

      -- check valid recon field
      if v_recon_field <> '' or v_dataset_field_name <> '' then
        if v_tran_gid > 0 and v_tranbrkp_gid > 0 then
          set v_sql = concat("select ",v_recon_field," into @old_field_value from ",v_tranbrkp_table,"
              where tran_gid = ",cast(v_tran_gid as nchar),"
              and tranbrkp_gid = ",cast(v_tranbrkp_gid as nchar),"
              and delete_flag = 'N'");

          call pr_run_sql1(v_sql,@msg,@result);

          set v_sql = concat("update ",v_tranbrkp_table," set
                ",v_recon_field," = ",v_field_value,"
              where tran_gid = ",cast(v_tran_gid as nchar),"
              and tranbrkp_gid = ",cast(v_tranbrkp_gid as nchar),"
              and delete_flag = 'N'");
        elseif v_tran_gid > 0 and v_tranbrkp_gid = 0 then
          set v_sql = concat("select ",v_recon_field," into @old_field_value from ",v_tran_table,"
              where tran_gid = ",cast(v_tran_gid as nchar),"
              and delete_flag = 'N'");

          call pr_run_sql1(v_sql,@msg,@result);

          set v_sql = concat("update ",v_tran_table," set
                ",v_recon_field," = ",v_field_value,"
              where tran_gid = ",cast(v_tran_gid as nchar),"
              and delete_flag = 'N'");
        elseif v_dataset_field_name <> '' then
          -- update dataset field
          set v_sql = concat("select ",v_dataset_field_name," into @old_field_value from ",v_dataset_code,"
              where dataset_gid = ",cast(v_dataset_gid as nchar),"
              and delete_flag = 'N'");

          call pr_run_sql1(v_sql,@msg,@result);

          set v_sql = concat("update ",v_dataset_code," set
                ",v_dataset_field_name," = ",v_field_value,"
              where dataset_gid = ",cast(v_dataset_gid as nchar),"
              and delete_flag = 'N'");
        else
          set v_sql = "select 1";
        end if;

        call pr_run_sql1(v_sql,@msg,@result);

        -- update completed cases
        update recon_trn_tfieldupdate set
          old_field_value = @old_field_value,
          update_status = 'C'
        where scheduler_gid = in_scheduler_gid
        and tran_gid = v_tran_gid
        and tranbrkp_gid = v_tranbrkp_gid
        and update_status = 'P'
        and delete_flag = 'N';
      else
        -- update failed cases
        update recon_trn_tfieldupdate set
          update_status = 'F'
        where scheduler_gid = in_scheduler_gid
        and tran_gid = v_tran_gid
        and tranbrkp_gid = v_tranbrkp_gid
        and update_status = 'P'
        and delete_flag = 'N';
      end if;
    end loop updatefield_loop;

    close updatefield_cursor;
  end updatefield_block;

  insert into recon_trn_tfieldupdatepost
    select * from recon_trn_tfieldupdate
    where scheduler_gid = in_scheduler_gid
    and delete_flag = 'N';

  delete from recon_trn_tfieldupdate
  where scheduler_gid = in_scheduler_gid
  and delete_flag = 'N';

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;