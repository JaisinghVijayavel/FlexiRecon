DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_pdtranreport` $$
CREATE PROCEDURE `pr_run_pdtranreport`(
  in in_recon_code varchar(32),
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
    Created Date : 28-11-2024

    Updated By : Vijayavel
    updated Date : 20-03-2025

    Version : 3
  */

  declare v_count int default 0;
  declare v_sql text default '';

  declare v_pdrecon_code text default '';
	declare v_tran_table text default '';
	declare v_tranko_table text default '';

  declare v_concurrent_ko_flag text default '';

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  drop temporary table if exists recon_tmp_tpdtran;

  CREATE temporary TABLE recon_tmp_tpdtran select * from recon_trn_ttran where 1 = 2;

  alter table recon_tmp_tpdtran ENGINE = MyISAM;
  alter table recon_tmp_tpdtran add primary key(tran_gid);

  create index idx_excp_value on recon_tmp_tpdtran(excp_value);
  create index idx_tran_date on recon_tmp_tpdtran(tran_date);
  create index idx_recon_code on recon_tmp_tpdtran(recon_code);
  create index idx_dataset_code on recon_tmp_tpdtran(recon_code,dataset_code);

  -- delete record
  if in_job_gid = 0 and in_rptsession_gid = 0 then
    delete from recon_rpt_ttran
    where user_code = in_user_code
    and job_gid = 0
    and rptsession_gid = 0;
  end if;

  -- concurrent KO flag
  set v_concurrent_ko_flag = fn_get_configvalue('concurrent_ko_flag');

  if v_concurrent_ko_flag = 'Y' then
		-- pdrecon block
		pdrecon_block:begin
			declare pdrecon_done int default 0;

			declare pdrecon_cursor cursor for
				select pdrecon_code from recon_mst_tpdrecon
				where active_status = 'Y'
				and delete_flag = 'N';

			declare continue handler for not found set pdrecon_done=1;

			open pdrecon_cursor;

			pdrecon_loop: loop
				fetch pdrecon_cursor into v_pdrecon_code;
				if pdrecon_done = 1 then leave pdrecon_loop; end if;

        set v_tran_table = concat(v_pdrecon_code,'_tran');
        set v_tranko_table = concat(v_pdrecon_code,'_tranko');

				-- transfer to temporary table
				set v_sql = concat("insert into recon_tmp_tpdtran
					select z.* from (
					select
						a.*
					from ",v_tran_table," as a
          left join recon_mst_tdataset as b on a.dataset_code = b.dataset_code and b.delete_flag = 'N'
					where 1 = 1
					and a.recon_code = '",v_pdrecon_code,"'
					and a.delete_flag = 'N' ", in_condition,"

					union all

					select
						a.*
					from ",v_tranko_table," as a
          left join recon_mst_tdataset as b on a.dataset_code = b.dataset_code and b.delete_flag = 'N'
					where 1 = 1
					and a.recon_code = '",v_pdrecon_code,"'
					and a.delete_flag = 'N' ", in_condition,"
					LOCK IN SHARE MODE) as z
				");

				call pr_run_sql(v_sql,@msg,@result);

			end loop pdrecon_loop;
			close pdrecon_cursor;
		end pdrecon_block;
  else
		-- transfer to temporary table
		set v_sql = concat("insert into recon_tmp_tpdtran
			select z.* from (
			select
				a.*
			from recon_trn_ttran as a
			inner join recon_mst_tpdrecon as p on a.recon_code = p.pdrecon_code
				and p.active_status = 'Y'
				and p.delete_flag = 'N'
      left join recon_mst_tdataset as b on a.dataset_code = b.dataset_code and b.delete_flag = 'N'
			where a.delete_flag = 'N' ", in_condition,"

			union all

			select
				a.*
			from recon_trn_ttranko as a
			inner join recon_mst_tpdrecon as p on a.recon_code = p.pdrecon_code
				and p.active_status = 'Y'
				and p.delete_flag = 'N'
      left join recon_mst_tdataset as b on a.dataset_code = b.dataset_code and b.delete_flag = 'N'
			where a.delete_flag = 'N' ", in_condition,"
			LOCK IN SHARE MODE) as z
		");

    call pr_run_sql2(v_sql,@msg,@result);
  end if;

  -- calc exception value based on roundoff value
  update recon_tmp_tpdtran set
    excp_value = excp_value - roundoff_value;

  -- transfer records to report table
  set @rec_slno := 0;

  set v_sql = concat("insert into recon_rpt_ttran
		select
      ",cast(in_rptsession_gid as nchar),",
		  ",cast(in_job_gid as nchar)," as job_gid,
		  @rec_slno:=@rec_slno+1,
      '", in_user_code ,"',
      b.dataset_name,
      null as match_gid,
      a.*
		from recon_tmp_tpdtran as a
    left join recon_mst_tdataset as b on a.dataset_code = b.dataset_code and b.delete_flag = 'N' ",in_sorting_order);

  call pr_run_sql(v_sql,@msg,@result);

  drop temporary table if exists recon_tmp_tpdtran;

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;