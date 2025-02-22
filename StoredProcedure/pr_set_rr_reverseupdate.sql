DELIMITER $$

DROP PROCEDURE IF EXISTS pr_set_rr_reverseupdate $$
CREATE PROCEDURE pr_set_rr_reverseupdate
(
  in_recon_code varchar(32)
)
me:begin
  /*
    Created By : Vijayavel
    Created Date : 21-02-2025

    Updated By :
    Updated Date :

    Version : 1
  */

  declare v_sql text default '';
	declare v_ds_db_name text default '';
	declare v_rr_dscode text default '';
	
	-- get dataset db name
	set v_ds_db_name = fn_get_configvalue('dataset_db_name');

	-- get RR dataset code
	select
		a.dataset_code
	into
		v_rr_dscode
	from recon_mst_trecondataset as a 
	inner join recon_mst_tdataset as b on a.dataset_code = b.dataset_code
		and b.dataset_name like 'ReceiptReport-%-NI'
		and b.active_status = 'Y'
		and b.delete_flag = 'N'
	where a.recon_code = in_recon_code 
	and a.dataset_type = 'L' 
	and a.active_status = 'Y'
	and a.delete_flag = 'N';

	set v_rr_dscode = ifnull(v_rr_dscode,'');
	
	if v_rr_dscode = '' THEN
		leave me;
	end if;
	
	if v_ds_db_name <> '' THEN
		set v_rr_dscode = concat(v_ds_db_name,'.',v_rr_dscode);
	end if;

  drop temporary table if exists recon_tmp_ttranrr;

	create temporary table recon_tmp_ttranrr select * from recon_trn_ttran where 1 = 2;
	alter table recon_tmp_ttranrr ENGINE = MyISAM;
  alter table recon_tmp_ttranrr add primary key(tran_gid);
	create index idx_recon_code on recon_tmp_ttranrr(recon_code);
	create index idx_rr_no on recon_tmp_ttranrr(col14(255));

	insert into recon_tmp_ttranrr
		select a.* from
		(
			select * from recon_trn_ttranko
			where recon_code = in_recon_code
			and col14 <> ''
			and delete_flag = 'N'
			LOCK IN SHARE MODE
		) as a;

	set v_sql = '';
	set v_sql = concat(v_sql,"update recon_tmp_ttranrr as a
		inner join ",v_rr_dscode," as b on a.col14 = b.col1
			and b.col11 = 'ACTIVE'
			and (b.col35 is null
			or b.col35 = '')
		set b.col35 = cast(a.tran_gid as nchar)");

  set @sqlrr = v_sql;
  prepare sqlrr_stmt from @sqlrr;
  execute sqlrr_stmt;
  deallocate prepare sqlrr_stmt;

  drop temporary table if exists recon_tmp_ttranrr;
end $$

DELIMITER ;