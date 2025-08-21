DELIMITER $$
DROP PROCEDURE IF EXISTS `pr_set_iutfieldupdate` $$
CREATE procedure `pr_set_iutfieldupdate`
(
  in in_scheduler_gid int,
  in in_job_gid int,
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32),
  out out_msg text,
  out out_result int
)
me:begin
  /*
    Created By : Vijayavel
    Created Date : 19-06-2025

    Updated By : Vijayavel
    Updated Date : 21-08-2025

    Version : 2
  */

  declare v_pd_tran_gid int default 0;
  declare v_pd_tranbrkp_gid int default 0;

  declare v_tran_gid int default 0;
  declare v_tranbrkp_gid int default 0;

  declare v_recon_code text default '';

  declare v_recon_field_desc text default '';

  declare v_field_desc text default '';
  declare v_field_value text default '';
  declare v_old_field_value text default '';

  declare v_recon_field text default '';
  declare v_recon_field_type text default '';

  declare v_tran_table text default '';
  declare v_tranbrkp_table text default '';

  declare v_concurrent_ko_flag text default '';

  declare v_sql text default '';
  declare v_txt text default '';

  -- col1 = tran_gid
  -- col2 = tranbrkp_gid

  -- update field
  updatefield_block:begin
    declare updatefield_done int default 0;
    declare updatefield_cursor cursor for
      select
        pd_tran_gid,pd_tranbrkp_gid,recon_code,field_desc,field_value
      from recon_trn_tiutfieldupdate
      where scheduler_gid = in_scheduler_gid
      and delete_flag = 'N';
    declare continue handler for not found set updatefield_done=1;

    open updatefield_cursor;

    updatefield_loop: loop
      fetch updatefield_cursor into v_pd_tran_gid,v_pd_tranbrkp_gid,
                                    v_recon_code,v_field_desc,v_field_value;

      if updatefield_done = 1 then leave updatefield_loop; end if;

      set v_recon_code = ifnull(v_recon_code,'');

      set v_field_desc = ifnull(v_field_desc,'');
      set v_field_value = ifnull(v_field_value,'');

      -- get recon field and type
      -- call pr_get_fieldname(v_recon_code,v_recon_field_desc,v_recon_field,v_recon_field_type);

      -- concurrent KO flag
      set v_concurrent_ko_flag = fn_get_configvalue('concurrent_ko_flag');

      if v_concurrent_ko_flag = 'Y' then
        set v_tran_table = concat(v_recon_code,'_tran');
	      set v_tranbrkp_table = concat(v_recon_code,'_tranbrkp');
      else
	      set v_tran_table = 'recon_trn_ttran';
	      set v_tranbrkp_table = 'recon_trn_ttranbrkp';
      end if;

      if v_pd_tran_gid > 0 or v_pd_tranbrkp_gid > 0 then
        if v_recon_code = '' then
          if v_pd_tran_gid > 0 then
            set v_sql = concat("select recon_code into @recon_code from ",v_tran_table,"
              where col1 = ",cast(v_pd_tran_gid as nchar),"
              and delete_flag = 'N'");
          else
            set v_sql = concat("select recon_code into @recon_code from ",v_tranbrkp_table,"
              where col2 = ",cast(v_pd_tranbrkp_gid as nchar),"
              and delete_flag = 'N'");
          end if;

          call pr_run_sql1(v_sql,@msg,@result);

          set v_recon_code = ifnull(@recon_code,'');
        end if;

        set v_recon_field_desc = v_field_desc;
        set v_recon_field = fn_get_reconfieldfromdesc(v_recon_code,v_recon_field_desc);

				-- check if any job is running
				if exists(select job_gid from recon_trn_tjob
					where recon_code = v_recon_code
					and jobtype_code in ('A','M','U','T','UJ')
					and job_status in ('I','P')
          and job_gid <> in_job_gid
					and delete_flag = 'N') then

					select group_concat(cast(job_gid as nchar)) into v_txt from recon_trn_tjob
					where recon_code = v_recon_code
					and jobtype_code in ('A','M','U','T','UJ')
					and job_status in ('I','P')
          and job_gid <> in_job_gid
					and delete_flag = 'N';

					set out_msg = concat('KO/Undo KO/Field Update/Theme is already running in the job id ', v_txt ,' ! ');
					set out_result = 0;
          leave me;
				end if;
      else
        set v_recon_field = '';
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
      if v_recon_field <> '' then
        set v_tran_gid = 0;
        set v_tranbrkp_gid = 0;

        if v_pd_tranbrkp_gid > 0 then
          set v_sql = concat("select ",v_recon_field,",tran_gid,tranbrkp_gid into
              @old_field_value,@tran_gid,@tranbrkp_gid from ",v_tranbrkp_table,"
              where recon_code = '",v_recon_code,"'
              and col1 = ",cast(v_pd_tran_gid as nchar),"
              and col2 = ",cast(v_pd_tranbrkp_gid as nchar),"
              and delete_flag = 'N'");

          call pr_run_sql1(v_sql,@msg,@result);

          set v_old_field_value = ifnull(@old_field_value,'');
          set v_tran_gid = ifnull(@tran_gid,0);
          set v_tranbrkp_gid = ifnull(@tranbrkp_gid,0);

          set v_sql = concat("update ",v_tranbrkp_table," set
                ",v_recon_field," = ",v_field_value,"
              where tranbrkp_gid = ",cast(v_tranbrkp_gid as nchar),"
              and delete_flag = 'N'");
        elseif v_pd_tran_gid > 0 and v_pd_tranbrkp_gid = 0 then
          set v_sql = concat("select ",v_recon_field,",tran_gid into @old_field_value,@tran_gid from ",v_tran_table,"
              where recon_code = '",v_recon_code,"' 
              and col1 = ",cast(v_pd_tran_gid as nchar),"
              and delete_flag = 'N'");

          call pr_run_sql1(v_sql,@msg,@result);

          set v_old_field_value = ifnull(@old_field_value,'');
          set v_tran_gid = ifnull(@tran_gid,0);

          set v_sql = concat("update ",v_tran_table," set
                ",v_recon_field," = ",v_field_value,"
              where tran_gid = ",cast(v_tran_gid as nchar),"
              and delete_flag = 'N'");
        else
          set v_sql = "select 1";
        end if;

        call pr_run_sql1(v_sql,@msg,@result);

        -- update completed cases
        update recon_trn_tiutfieldupdate set
          old_field_value = v_old_field_value,
          tran_gid = v_tran_gid,
          tranbrkp_gid = v_tranbrkp_gid,
          update_status = 'C'
        where scheduler_gid = in_scheduler_gid
        and pd_tran_gid = v_pd_tran_gid
        and pd_tranbrkp_gid = v_pd_tranbrkp_gid
        and field_desc = v_field_desc
        and update_status = 'P'
        and delete_flag = 'N';
      else
        -- update failed cases
        update recon_trn_tiutfieldupdate set
          update_status = 'F'
        where scheduler_gid = in_scheduler_gid
        and tran_gid = v_pd_tran_gid
        and tranbrkp_gid = v_pd_tranbrkp_gid
        and update_status = 'P'
        and delete_flag = 'N';
      end if;
    end loop updatefield_loop;

    close updatefield_cursor;
  end updatefield_block;

  insert into recon_trn_tiutfieldupdatepost
    select * from recon_trn_tiutfieldupdate
    where scheduler_gid = in_scheduler_gid
    and delete_flag = 'N';

  delete from recon_trn_tiutfieldupdate
  where scheduler_gid = in_scheduler_gid
  and delete_flag = 'N';

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;