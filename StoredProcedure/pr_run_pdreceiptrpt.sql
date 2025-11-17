DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_pdreceiptrpt` $$
CREATE PROCEDURE `pr_run_pdreceiptrpt`(
  in in_recon_code varchar(32),
  in in_report_code varchar(32),
  in in_job_gid int,
  in in_rptsession_gid int,
  in in_condition text,
  in in_sorting_order text,
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
  /*
    Created By : Vijayavel
    Created Date : 14-11-2025

    Updated By :
    updated Date :

    Version : 1
  */

  declare v_tran_field text default '';

  declare v_count int default 0;
  declare v_sql text default '';

  declare v_pddataset_code text default '';
  declare v_dataset_db_name text default '';
  declare v_dataset_tb_name text default '';
  declare v_tb_name text default '';

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  set v_dataset_db_name = fn_get_configvalue('dataset_db_name');

  if v_dataset_db_name = '' then
    set v_dataset_db_name = database();
  end if;

	set v_dataset_tb_name = concat(v_dataset_db_name,'.',in_report_code);

  set v_sql = concat("delete from ",v_dataset_tb_name," where user_code = '",in_user_code,"'");
  call pr_run_sql2(v_sql,@out_msg,@out_result);

	-- pdrecon block
	pddataset_block:begin
		declare pddataset_done int default 0;

		declare pddataset_cursor cursor for
			select dataset_code from recon_mst_tdataset
			where dataset_name like '%RECEIPT%-NI%'
			and active_status = 'Y'
			and delete_flag = 'N';

		declare continue handler for not found set pddataset_done=1;

		open pddataset_cursor;

		pddataset_loop: loop
			fetch pddataset_cursor into v_pddataset_code;
			if pddataset_done = 1 then leave pddataset_loop; end if;

			set v_tb_name = concat(v_dataset_db_name,'.',v_pddataset_code);

      if v_tran_field = '' then
        -- get table column
        SELECT
	        group_concat(t.COLUMN_NAME) into v_tran_field
        FROM information_schema.columns as t
        WHERE t.table_schema=v_dataset_db_name
        AND t.table_name = v_pddataset_code
        AND t.COLUMN_NAME <> 'dataset_gid';
      end if;

      set v_tran_field = ifnull(v_tran_field,'');

      if v_tran_field <> '' then
			  -- transfer tran records to report table
			  set v_sql = concat('insert into ',v_dataset_tb_name,'(rptsession_gid,job_gid,user_code,dataset_name,',v_tran_field,') ');
			  set v_sql = concat(v_sql,'select z.* from (');
			  set v_sql = concat(v_sql,'select ');
			  set v_sql = concat(v_sql,cast(in_rptsession_gid as nchar),' as rptsession_gid,');
			  set v_sql = concat(v_sql,cast(in_job_gid as nchar),' as job_gid,');
			  set v_sql = concat(v_sql,char(39),in_user_code,char(39),' as user_code,');
			  set v_sql = concat(v_sql,'b.dataset_name as ds_name,');
			  set v_sql = concat(v_sql,concat('a.',replace(v_tran_field,',',',a.')),' from ',v_tb_name,' as a ');

			  set v_sql = concat(v_sql,'left join recon_mst_tdataset as b on b.dataset_code = ''',v_pddataset_code,''' ');
			  set v_sql = concat(v_sql,'where 1 = 1 ');

			  set v_sql = concat(v_sql,in_condition,' ');

			  set v_sql = concat(v_sql,'and a.delete_flag = ''N'' ');
			  set v_sql = concat(v_sql,' ',in_sorting_order);
			  set v_sql = concat(v_sql,' limit 10000 LOCK IN SHARE MODE) as z ');

			  call pr_run_sql2(v_sql,@out_msg,@out_result);
      end if;
		end loop pddataset_loop;

		close pddataset_cursor;
	end pddataset_block;

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;