DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_reconrowqcdlist` $$
CREATE PROCEDURE `pr_get_reconrowqcdlist`
(
  in in_recon_code varchar(32),
  in in_tran_gid int,
  in in_tranbrkp_gid int,
  out out_msg text,
  out out_result int
)
BEGIN
  /*
    Created By : Vijayavel
    Created Date: 03-12-2025

    Updated By : Vijayavel
    updated Date :

	  Version - 1
  */

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';

	declare v_field_name text default '';
	declare v_field_desc text default '';
	declare v_field_value text default '';
	declare v_qcd_multivalue_flag text default '';
  declare v_concurrent_ko_flag text default '';

	declare v_sql text default '';

	drop temporary table if exists recon_tmp_trowvalue;
	drop temporary table if exists recon_tmp_treconcol;

	CREATE TEMPORARY TABLE recon_tmp_trowvalue
	(
		rowvalue_gid int unsigned not null AUTO_INCREMENT,
		tran_gid int not null default 0,
		tranbrkp_gid int not null default 0,
		field_name varchar(255) default null,
		field_desc varchar(255) default null,
		field_value text default null,
    qcd_multivalue_flag varchar(32) default null,
		PRIMARY KEY (rowvalue_gid),
		key idx_field_name(field_name)
	) ENGINE = MyISAM;

	CREATE TEMPORARY TABLE recon_tmp_treconcol
	(
		field_name varchar(255) not null,
		field_desc varchar(255) not null,
    qcd_multivalue_flag varchar(32) default null,
		PRIMARY KEY (field_name)
	) ENGINE = MyISAM;

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

	-- insert system fields
  if in_tranbrkp_gid > 0 then
	  -- insert recon fields
	  insert ignore into recon_tmp_treconcol (field_name,field_desc,qcd_multivalue_flag)
		  select recon_field_name,recon_field_desc,qcd_multivalue_flag from recon_mst_treconfield
		  where recon_code = in_recon_code
      and recon_field_type = 'QCD'
      and recon_field_name not in
      (
        'bal_value_debit','bal_value_credit'
      )
		  and active_status = 'Y'
		  and delete_flag = 'N'
		  order by display_order;
  else
	  -- insert recon fields
	  insert ignore into recon_tmp_treconcol (field_name,field_desc,qcd_multivalue_flag)
		  select recon_field_name,recon_field_desc,qcd_multivalue_flag from recon_mst_treconfield
		  where recon_code = in_recon_code
      and recon_field_type = 'QCD'
		  and active_status = 'Y'
		  and delete_flag = 'N'
		  order by display_order;
  end if;


	-- reconfield block
	reconfield_block:begin
		declare reconfield_done int default 0;
		declare reconfield_cursor cursor for
			select field_name,field_desc,qcd_multivalue_flag from recon_tmp_treconcol;
		declare continue handler for not found set reconfield_done=1;

		open reconfield_cursor;

		reconfield_loop: loop
			fetch reconfield_cursor into v_field_name,v_field_desc,v_qcd_multivalue_flag;
			if reconfield_done = 1 then leave reconfield_loop; end if;

			set v_field_name = ifnull(v_field_name,'');
			set v_field_desc = ifnull(v_field_desc,'');
			set v_qcd_multivalue_flag = ifnull(v_qcd_multivalue_flag,'');

			set @v_field_value  = '';

			if in_tranbrkp_gid > 0 then
				set v_sql = concat("select cast(",v_field_name," as nchar) into @v_field_value
					from ",v_tranbrkp_table,"
					where tranbrkp_gid = ",cast(in_tranbrkp_gid as nchar),"
					and delete_flag = 'N'");
			else
				set v_sql = concat("select cast(",v_field_name," as nchar) into @v_field_value
					from ",v_tran_table,"
					where tran_gid = ",cast(in_tran_gid as nchar),"
					and delete_flag = 'N'");
			end if;

			set @v_sql1 = v_sql;
			prepare sql11_stmt from @v_sql1;
			execute sql11_stmt;
			deallocate prepare sql11_stmt;

      set v_field_value = @v_field_value;
			set v_field_value = ifnull(v_field_value,'');

			-- insert the return VALUE
			insert into recon_tmp_trowvalue
			(
				tran_gid,
				tranbrkp_gid,
				field_name,
				field_desc,
				field_value,
        qcd_multivalue_flag
			)
			select in_tran_gid,
						 in_tranbrkp_gid,
						 v_field_name,
						 v_field_desc,
						 v_field_value,
             v_qcd_multivalue_flag;
		end loop reconfield_loop;

		close reconfield_cursor;
	end reconfield_block;

  -- show selected line exception value
  if in_tranbrkp_gid > 0 then
    call pr_run_dynamicreport('','',in_recon_code,'RPT_EXCP_WITHBRKP','',
                             concat( 'and a.tranbrkp_gid = ',cast(in_tranbrkp_gid as nchar),' '),
                             false,'','','',@msg,@result);
  else
    call pr_run_dynamicreport('','',in_recon_code,'RPT_EXCP_WITHBRKP','',
                             concat( 'and a.tran_gid = ',cast(in_tran_gid as nchar),' '),
                             false,'','','',@msg,@result);
  end if;

  -- show qcd field list
	select * from recon_tmp_trowvalue order by rowvalue_gid;

	drop temporary table if exists recon_tmp_trowvalue;
	drop temporary table if exists recon_tmp_treconcol;

  set out_result = 1;
  set out_msg = 'Success';
END $$

DELIMITER ;