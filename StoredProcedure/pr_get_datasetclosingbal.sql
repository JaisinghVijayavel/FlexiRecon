DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_datasetclosingbal` $$
CREATE PROCEDURE `pr_get_datasetclosingbal`
(
  in in_recon_code varchar(32),
  in in_period_to date
)
me:BEGIN
  declare v_recon_name text default '';

  drop temporary table if exists recon_tmp_taccbal;

  CREATE temporary TABLE recon_tmp_taccbal(
    dataset_code varchar(32),
    dataset_type varchar(32),
    tran_date date,
    PRIMARY KEY (dataset_code,tran_date)
  ) ENGINE = MyISAM;

  -- get recon name
  select
    recon_name into v_recon_name
  from recon_mst_trecon
  where recon_code = in_recon_code
  and delete_flag = 'N';

	insert into recon_tmp_taccbal(dataset_code,dataset_type,tran_date)
	select
		a.dataset_code,
    a.dataset_type,
    max(b.tran_date) as tran_date
	from recon_mst_trecondataset as a
	inner join recon_trn_taccbal as b on a.dataset_code = b.dataset_code
		and b.tran_date <= in_period_to
		and b.delete_flag = 'N'
	where a.recon_code = 'RE098'
	and a.dataset_type in ('B','T')
	and a.active_status = 'Y'
	and a.delete_flag = 'N'
	group by a.dataset_code;

	select
		a.tran_date as 'Tran Date',
    v_recon_name as 'Recon Name',
		a.dataset_code as 'Dataset Code',
		c.dataset_name as 'Dataset Name',
    case
      when a.dataset_type = 'B' then 'Base'
      when a.dataset_type = 'T' then 'Target'
    end as 'Dataset Type',
		b.bal_value as 'Balance'
	from recon_tmp_taccbal as a
	inner join recon_trn_taccbal as b on a.dataset_code = b.dataset_code
		and a.tran_date = b.tran_date
		and b.delete_flag = 'N'
	left join recon_mst_tdataset as c on a.dataset_code = c.dataset_code
		and c.active_status = 'Y'
		and c.delete_flag = 'N'
  order by a.dataset_type;

  drop temporary table if exists recon_tmp_taccbal;
end $$

DELIMITER ;