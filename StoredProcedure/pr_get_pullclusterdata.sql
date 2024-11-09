DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_pullclusterdata` $$
CREATE PROCEDURE `pr_get_pullclusterdata`
(
  in in_recon_code text,
  in in_cluster_name text,
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_pdrecon_code varchar(32) default '';
  declare v_dataset_code varchar(32) default '';
  declare v_sql text default '';
  declare v_tran_table text default '';
  declare v_tranbrkp_table text default '';

  /*
    set v_tran_table = concat(in_recon_code,'_tran');
    set v_tranbrkp_table = concat(in_recon_code,'_tranbrkp');
  */

  set v_tran_table = 'recon_trn_ttran';
  set v_tranbrkp_table = 'recon_trn_ttranbrkp';

  -- get recon dataset code
  select
    dataset_code into v_dataset_code
  from recon_mst_trecondataset
  where recon_code = in_recon_code
  and dataset_type = 'B'
  and active_status = 'Y'
  and delete_flag = 'N'
  limit 0,1;

  set v_dataset_code = ifnull(v_dataset_code,'');

  -- clear recon data
  set v_sql = concat("delete from $TABLE$
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N'
    ");

  call pr_run_sql2(replace(v_sql,'$TABLE$',v_tran_table),@msg,@result);
  call pr_run_sql2(replace(v_sql,'$TABLE$',v_tranbrkp_table),@msg,@result);

	-- pdrecon block
	pdrecon_block:begin
		declare pdrecon_done int default 0;

		declare pdrecon_cursor cursor for
			select
				pdrecon_code
			from recon_mst_tpdrecon
			where cluster_name = in_cluster_name
			and active_status = 'Y'
			and delete_flag = 'N';

		declare continue handler for not found set pdrecon_done=1;

		open pdrecon_cursor;

		pdrecon_loop: loop
			fetch pdrecon_cursor into v_pdrecon_code;

			if pdrecon_done = 1 then leave pdrecon_loop; end if;

      set v_pdrecon_code = ifnull(v_pdrecon_code,'');

      -- pull data from pd recon
      set v_sql = concat("insert into ",v_tran_table,"
        (
          recon_code,
          dataset_code,
          scheduler_gid,
					col1,
					col3,
					col4,
					col5,
					col6,
					col7,
					col8,
					col9,
					col10,
					col11,
					col12,
					col13,
					col14,
					col15,
					col16,
					col17,
					col18,
					col19,
					col20,
					col21,
					col22,
					col23,
					col24,
					col25,
					col26,
					col27,
					col28,
					col29,
					col30,
					col31,
					col32,
					col33,
					col34,
					col35,
					col36,
					col37,
					col38
        )
        select
          '",in_recon_code,"',
          '",v_dataset_code,"',
          scheduler_gid,
					cast(tran_gid as nchar),
					dataset_code,
					cast(tran_date as nchar),
					fn_get_datasetname(dataset_code),
					dataset_code,
					fn_get_datasetname(dataset_code),
					cast(tran_value as nchar),
					cast(excp_value as nchar),
					cast(mapped_value as nchar),
					tran_acc_mode,
					cast(tran_mult as nchar),
					theme_code,
					tran_remark2,
					cast(roundoff_value as nchar),
					col1,
					cast(value_debit as nchar),
					cast(value_credit as nchar),
					col2,
					col3,
					col4,
					col5,
					col6,
					col7,
					col8,
					col10,
					col9,
					col11,
					col13,
					col14,
					col15,
					col16,
					col17,
					col18,
					col19,
					col20,
					col12,
					recon_code
        from ",v_tran_table,"
        where recon_code = '",v_pdrecon_code,"'
        and excp_value <> 0
        and delete_flag = 'N'
        ");

      call pr_run_sql2(v_sql,@msg,@result);

      -- tranbrkp table
      set v_sql = concat("insert into ",v_tran_table,"
        (
          recon_code,
          dataset_code,
          scheduler_gid,
					col1,
					col2,
					col3,
					col4,
					col5,
					col6,
					col7,
					col8,
					col9,
					col10,
					col11,
					col12,
					col13,
					col14,
					col15,
					col16,
					col17,
					col18,
					col19,
					col20,
					col21,
					col22,
					col23,
					col24,
					col25,
					col26,
					col27,
					col28,
					col29,
					col30,
					col31,
					col32,
					col33,
					col34,
					col35,
					col36,
					col37,
					col38
        )
        select
          '",in_recon_code,"',
          '",v_dataset_code,"',
          scheduler_gid,
					cast(tran_gid as nchar),
					cast(tranbrkp_gid as nchar),
					dataset_code,
					cast(tran_date as nchar),
					fn_get_datasetname(dataset_code),
					tranbrkp_dataset_code,
					fn_get_datasetname(tranbrkp_dataset_code),
					cast(tran_value as nchar),
					cast(excp_value as nchar),
					cast(mapped_value as nchar),
					tran_acc_mode,
					cast(tran_mult as nchar),
					theme_code,
					tran_remark2,
					cast(roundoff_value as nchar),
					col1,
					cast(value_debit as nchar),
					cast(value_credit as nchar),
					col2,
					col3,
					col4,
					col5,
					col6,
					col7,
					col8,
					col10,
					col9,
					col11,
					col13,
					col14,
					col15,
					col16,
					col17,
					col18,
					col19,
					col20,
					col12,
					recon_code
        from ",v_tranbrkp_table,"
        where recon_code = '",v_pdrecon_code,"'
        and excp_value <> 0
        and delete_flag = 'N'
        ");

      call pr_run_sql2(v_sql,@msg,@result);
		end loop pdrecon_loop;

		close pdrecon_cursor;
	end pdrecon_block;

  set out_result = 1;
  set out_msg = 'Success';
end $$

DELIMITER ;