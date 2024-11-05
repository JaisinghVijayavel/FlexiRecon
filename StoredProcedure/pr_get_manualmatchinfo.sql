DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_manualinfo` $$
CREATE PROCEDURE `pr_get_manualinfo`(
  in in_scheduler_gid text,
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_recon_code varchar(32) default '';
  declare v_recontype_code varchar(32) default '';
  declare v_dataset_code varchar(32) default '';

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';

  declare v_sql text default '';

	set v_tran_table = 'recon_trn_ttran';
	set v_tranbrkp_table = 'recon_trn_ttranbrkp';

    -- dataset_code
  select b.target_dataset_code into v_dataset_code from con_trn_tscheduler as a
  inner join con_mst_tpipeline as b on a.pipeline_code = b.pipeline_code and b.delete_flag = 'N'
  where a.scheduler_gid = in_scheduler_gid
  and a.delete_flag = 'N';

  -- recon code
  if v_dataset_code = 'KOMANUAL' then
    select
      a.recon_code,
      b.recontype_code
    into
      v_recon_code,
      v_recontype_code
    from recon_trn_tmanualtran as a
    inner join recon_mst_trecon as b on a.recon_code = b.recon_code and b.delete_flag = 'N'
    where a.scheduler_gid = in_scheduler_gid
    and a.delete_flag = 'N'
    limit 0,1;
  elseif v_dataset_code = 'POSTMANUAL' then
    select
      a.recon_code,
      b.recontype_code
    into
      v_recon_code,
      v_recontype_code
    from recon_trn_tmanualtranbrkp as a
    inner join recon_mst_trecon as b on a.recon_code = b.recon_code and b.delete_flag = 'N'
    where a.scheduler_gid = in_scheduler_gid
    and a.delete_flag = 'N'
    limit 0,1;
  end if;

  set v_recon_code = ifnull(v_recon_code,'');

  -- return scheduler
  select
    scheduler_gid as 'Scheduler Id',
    scheduled_date as 'Scheduled Date',
    pipeline_code as 'Pipeline Code',
    scheduler_parameters as 'Scheduler Parameters',
    file_name as 'File Name',
    scheduler_start_date as 'Start Date',
    scheduler_end_date as 'Completed Date',
    scheduler_remark as 'Remark'
  from con_trn_tscheduler
  where scheduler_gid = in_scheduler_gid
  and scheduler_status = 'Completed'
  and delete_flag = 'N';

  -- recon info
  select
    concat(recon_code,'-',recon_name) as 'Recon Name',
    fn_get_mastername(recontype_code,'QCD_RC_RCON_TYPE') as 'Recon Type',
    recon_rule_version as 'Rule Version'
  from recon_mst_trecon
  where recon_code = v_recon_code
  and delete_flag = 'N';

  -- return manual match info
	if v_dataset_code = 'KOMANUAL' then
	  -- return dataset
		select distinct
			a.dataset_code as 'Dataset Code',
			b.dataset_name as 'Dataset Name',
			fn_get_mastername(c.dataset_type,'QCD_DS_TYPE') as 'Dataset Type'
		from recon_trn_tmanualtran as a
		inner join recon_mst_tdataset as b on a.dataset_code = b.dataset_code and b.delete_flag = 'N'
		inner join recon_mst_trecondataset as c on b.dataset_code = c.dataset_code
      and a.recon_code = c.recon_code
      and c.active_status = 'Y' 
			and c.delete_flag = 'N'
		where a.scheduler_gid = in_scheduler_gid
		and a.delete_flag = 'N';

		if v_recontype_code = 'W' or v_recontype_code = 'B' then
			-- Proof/BRS
			select
				count(distinct match_gid) as 'Match Count',
				sum(if(a.ko_acc_mode = 'D',1,0)) as 'DR Count',
				format(sum(if(a.ko_acc_mode = 'D',a.ko_value,0)),2,'en_IN') as 'DR Total',
				sum(if(a.ko_acc_mode = 'C',1,0)) as 'CR Count',
				format(sum(if(a.ko_acc_mode = 'C',a.ko_value,0)),2,'en_IN') as 'CR Total',
				format(sum(a.ko_value*a.ko_mult),2,'en_IN') as 'Difference'
			from recon_trn_tmanualtran as a
			inner join recon_trn_ttran as b on a.tran_gid = b.tran_gid
				and b.recon_code = v_recon_code
				and b.excp_value <> 0
				and b.delete_flag = 'N'
			where a.scheduler_gid = in_scheduler_gid
			and a.delete_flag = 'N';
		elseif v_recontype_code = 'I' or v_recontype_code = 'V' then
			-- Mirror/Value Based
			select
				count(distinct match_gid) as 'Match Count',
				sum(if(c.dataset_type = 'B',1,0)) as 'Base Count',
				sum(if(c.dataset_type = 'B',a.ko_value*a.ko_mult,0)) as 'Base Total',
				sum(if(c.dataset_type = 'T',1,0)) as 'Target Count',
				sum(if(c.dataset_type = 'T',a.ko_value*a.ko_mult,0)) as 'Target Total',
				sum(if(c.dataset_type = 'B',a.ko_value*a.ko_mult,0)) - sum(if(c.dataset_type = 'T',a.ko_value*a.ko_mult,0)) as 'Difference'
      from recon_trn_tmanualtran as a
			left join recon_trn_ttran as b on a.tran_gid = b.tran_gid
				and b.recon_code = v_recon_code
				and b.excp_value > 0
				and b.delete_flag = 'N'
			left join recon_mst_trecondataset as c on b.recon_code = c.recon_code
				and b.dataset_code = c.dataset_code
        and c.active_status = 'Y'
				and c.delete_flag = 'N'
			where a.scheduler_gid = in_scheduler_gid
			and a.delete_flag = 'N';
		elseif v_recontype_code = 'N' then
			-- Non Value Based
			select
				count(distinct match_gid) as 'Match Count',
				sum(if(c.dataset_type = 'B',1,0)) as 'Base Count',
				sum(if(c.dataset_type = 'T',1,0)) as 'Target Count'
			from recon_trn_tmanualtran as a
			inner join recon_trn_ttran as b on a.tran_gid = b.tran_gid
				and b.recon_code = v_recon_code
				and b.excp_value > 0
				and b.delete_flag = 'N'
			left join recon_mst_trecondataset as c on b.recon_code = c.recon_code
				and b.dataset_code = c.dataset_code
        and c.active_status = 'Y'
				and c.delete_flag = 'N'
			where a.scheduler_gid = in_scheduler_gid
			and a.delete_flag = 'N';
    else
			select
				count(distinct match_gid) as 'Match Count',
				sum(if(b.tran_acc_mode = 'D',1,0)) as 'DR Count',
				format(sum(if(b.tran_acc_mode = 'D',a.ko_value,0)),2,'en_IN') as 'DR Total',
				sum(if(b.tran_acc_mode = 'C',1,0)) as 'CR Count',
				format(sum(if(b.tran_acc_mode = 'C',a.ko_value,0)),2,'en_IN') as 'CR Total',
				format(sum(b.excp_value*b.tran_mult),2,'en_IN') as 'Difference'
			from recon_trn_tmanualtran as a
			inner join recon_trn_ttran as b on a.tran_gid = b.tran_gid
				and b.recon_code = v_recon_code
				and b.excp_value <> 0
				and b.delete_flag = 'N'
			where a.scheduler_gid = in_scheduler_gid
			and a.delete_flag = 'N';
		end if;
	elseif v_dataset_code = 'POSTMANUAL' then
	  -- return dataset
		select distinct
			a.dataset_code as 'Dataset Code',
			c.dataset_name as 'Dataset Name',
			fn_get_mastername(d.dataset_type,'QCD_DS_TYPE') as 'Dataset Type'
		from recon_trn_tmanualtranbrkp as a
		inner join recon_trn_ttranbrkp as b on a.tranbrkp_gid = a.tranbrkp_gid
			and b.delete_flag = 'N'
		left join recon_mst_tdataset as c on b.dataset_code = c.dataset_code and c.delete_flag = 'N'
		left join recon_mst_trecondataset as d on b.dataset_code = d.dataset_code
      and b.recon_code = d.recon_code
      and d.active_status = 'Y'
			and d.delete_flag = 'N'
		where a.scheduler_gid = in_scheduler_gid
		and a.delete_flag = 'N'

		union all

		select distinct
			b.tranbrkp_dataset_code as 'Dataset Code',
			c.dataset_name as 'Dataset Name',
			fn_get_mastername(d.dataset_type,'QCD_DS_TYPE') as 'Dataset Type'
		from recon_trn_tmanualtranbrkp as a
		inner join recon_trn_ttranbrkp as b on a.tranbrkp_gid = a.tranbrkp_gid
			and b.delete_flag = 'N'
		left join recon_mst_tdataset as c on b.tranbrkp_dataset_code = c.dataset_code and c.delete_flag = 'N'
		left join recon_mst_trecondataset as d on b.dataset_code = d.dataset_code
      and d.recon_code = b.recon_code
      and d.active_status = 'Y'
			and d.delete_flag = 'N'
		where a.scheduler_gid = in_scheduler_gid
		and a.delete_flag = 'N';

    -- return value
    select
      count(distinct a.tran_gid) as 'Match Count',
      count(*) as 'Tranbrkp Count',
      abs(sum(b.excp_value*b.tran_mult)) as 'Tranbrkp Total'
    from recon_trn_tmanualtranbrkp as a
    inner join recon_trn_ttranbrkp as b on a.tranbrkp_gid = b.tranbrkp_gid
      -- and b.recon_code = v_recon_code
      and b.excp_value > 0
      and b.delete_flag = 'N'
    left join recon_mst_trecondataset as c on b.recon_code = c.recon_code
      and b.dataset_code = c.dataset_code
      and c.active_status = 'Y'
      and c.delete_flag = 'N'
    where a.scheduler_gid = in_scheduler_gid
    and a.delete_flag = 'N';
	end if;
end $$

DELIMITER ;