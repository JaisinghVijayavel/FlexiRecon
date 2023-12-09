DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_fetch_recondetails` $$
CREATE PROCEDURE `pr_fetch_recondetails`
(
	in in_recon_code int(25),
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32)
)
me:BEGIN
	declare v_dataset_count int default 0;
	declare v_recon_code text default ''; 
	
	select
    a.recon_gid,
    a.recon_code,
    a.recon_name,
    a.recontype_code,
    b.recontype_desc,
		DATE_FORMAT(a.period_from,'%d/%m/%Y') as  period_from,
    ifnull(a.period_to,'') as period_to,
    a.until_active_flag,
    a.recon_value_flag,
    a.recon_value_field,
    a.recon_date_flag,
    a.recon_date_field,
    a.recon_automatch_partial,
    a.threshold_plus_value,
    a.threshold_minus_value,
		a.active_status,
		fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
  from recon_mst_trecon a
  inner join recon_mst_trecontype b on a.recontype_code = b.recontype_code
	where a.recon_gid =in_recon_code 
	and a.delete_flag = 'N';
	
	set v_recon_code = (select recon_code from recon_mst_trecon where recon_gid=in_recon_code);

	select 
		a.recondataset_gid,
		a.recon_code,
		a.dataset_code,
		b.dataset_name,
		a.dataset_type,
		fn_get_mastername(a.dataset_type, 'QCD_DS_TYPE') as dataset_type_desc,
		a.parent_dataset_code,
		ifnull(c.dataset_name,'') as parent_dataset_name,
		a.active_status,
		fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
	from recon_mst_trecondataset a
	inner join recon_mst_tdataset b on a.dataset_code = b.dataset_code
	left join recon_mst_tdataset c on a.parent_dataset_code = c.dataset_code and a.dataset_type='S'
	where a.recon_code = v_recon_code 
	and  a.active_status = 'Y' 
	and a.delete_flag = 'N';

  select
    count(*) into v_dataset_count
  from recon_mst_treconfield
  where recon_code=v_recon_code
  and delete_flag = 'N';

  set v_dataset_count = ifnull(v_dataset_count,0);

	-- dataset mapping
	if(v_dataset_count>0) then
		SET @sql = NULL;

		SELECT
			GROUP_CONCAT(DISTINCT
				CONCAT(
					'MAX(CASE WHEN dataset_name = ''',
					dataset_name,
					''' THEN field_desc END) AS ''',
					dataset_name ,''''
				)
			) INTO @dataset
		FROM
		(
			select distinct
				recon_name,a.recon_field_name,c.field_name,
				dataset_name,display_order,e.dataset_code,
        c.field_name as field_desc
			from recon_mst_treconfield a
			left join recon_mst_treconfieldmapping b on a.recon_field_name=b.recon_field_name
				and b.recon_code = a.recon_code
				and b.delete_flag = 'N'
			left join recon_mst_tdatasetfield c on c.dataset_table_field=b.dataset_field_name
				and c.delete_flag = 'N'
			left join recon_mst_trecon d on a.recon_code=d.recon_code
				and d.delete_flag = 'N'
			left join recon_mst_tdataset e on b.dataset_code=e.dataset_code
				and e.delete_flag = 'N'
			where a.recon_code= v_recon_code
			and a.delete_flag = 'N'
			order by dataset_name,display_order
		) a;

    set @dataset = ifnull(@dataset,'');

    if @dataset <> '' then
      set @dataset = concat(',',@dataset);
    end if;

		SET @sql = CONCAT('SELECT
      a.reconfield_gid,
      a.dataset_code,
      a.recon_code,
      a.recon_name,
      b.recon_field_desc as ''Recon Field Name'',
      a.display_order as ''Display Order''',
      @dataset,
      ' from recon_data_mapping as a
      inner join recon_mst_treconfield b on b.recon_field_name =a.recon_field_name
        and b.delete_flag = ''N''
      where a.recon_code=''',v_recon_code,'''
      GROUP BY a.recon_name, a.recon_field_name, a.display_order
      order by a.display_order;');
		-- Execute the dynamic SQL query to pivot the data.

		PREPARE dynamic_sql FROM @sql;
		EXECUTE dynamic_sql;
	else
		SELECT
      a.reconfield_gid,
			dataset_code,
			a.recon_code,
			recon_name,
			b.recon_field_desc as 'Recon Field Name',
			a.display_order as 'Display Order'
		from recon_data_mapping a
		inner join recon_mst_treconfield b on b.recon_field_name =a.recon_field_name
			and b.delete_flag = 'N'
		where a.recon_code=v_recon_code
			and b.delete_flag = 'N'
		GROUP BY recon_name, a.recon_field_name, a.display_order
		order by a.display_order;
	end if;

	select 
		recon_field_name,
		recon_field_desc,
		ifnull(recon_field_type,'') as recon_field_type  
	from recon_mst_treconfield 
	where recon_code=v_recon_code 
	and delete_flag = 'N';
END $$

DELIMITER ;