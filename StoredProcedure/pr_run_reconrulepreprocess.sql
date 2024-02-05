DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_reconrulepreprocess` $$
CREATE PROCEDURE `pr_run_reconrulepreprocess`(
  in in_recon_code text,
  in in_rule_code text,
  in in_period_from date,
  in in_period_to date,
  in in_automatch_flag char(1),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_update_recon_field text default '';
  declare v_lookup_recon_field text default '';
  declare v_process_apply_on text default '';
  declare v_process_method text default '';
  declare v_process_query text default '';
  declare v_process_function text default '';
  declare v_lookup_dataset_code text default '';
  declare v_lookup_dataset_field text default '';
  declare v_lookup_return_field text default '';

  declare v_recon_date_flag text default '';
  declare v_recon_date_field text default '';
  declare v_recon_date_condition text default '';

  declare v_source_dataset_code text default '';
  declare v_comparison_dataset_code text default '';
  declare v_dataset_code text default '';

  declare v_tran_table text default '';
  declare v_tranbrkp_table text default '';

  declare v_sql text default '';
  declare v_tran_sql text default '';
  declare v_tranbrkp_sql text default '';

  declare i int default 0;
  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  -- recon validation
  if not exists(select recon_code from recon_mst_trecon
    where recon_code = in_recon_code
    and active_status = 'Y'
    and period_from <= curdate()
    and (period_to >= curdate()
    or until_active_flag = 'Y')
    and delete_flag = 'N') then

    set out_msg = 'Invalid recon !';
    set out_result = 0;

    leave me;
  else
    select
      recon_date_flag,
      recon_date_field
    into
      v_recon_date_flag,
      v_recon_date_field
    from recon_mst_trecon
    where recon_code = in_recon_code
    and delete_flag = 'N';

    set v_recon_date_flag = ifnull(v_recon_date_flag,'');
    set v_recon_date_field = ifnull(v_recon_date_field,'');

    if v_recon_date_flag = 'Y' then
      set v_recon_date_condition = concat(v_recon_date_condition,' and ',v_recon_date_field,' >= ');
      set v_recon_date_condition = concat(v_recon_date_condition,char(39),date_format(in_period_from,'%Y-%m-%d'),char(39),' ');
      set v_recon_date_condition = concat(v_recon_date_condition,' and ',v_recon_date_field,' <= ');
      set v_recon_date_condition = concat(v_recon_date_condition,char(39),date_format(in_period_to,'%Y-%m-%d'),char(39),' ');
    end if;
  end if;

  -- rule validation
  if not exists(select rule_code from recon_mst_trule
    where rule_code = in_rule_code
    and active_status = 'Y'
    and period_from <= curdate()
    and (period_to >= curdate()
    or until_active_flag = 'Y')
    and delete_flag = 'N') then

    set out_msg = 'Invalid recon !';
    set out_result = 0;

    leave me;
  else
    select
      source_dataset_code,
      comparison_dataset_code
    into
      v_source_dataset_code,
      v_comparison_dataset_code
    from recon_mst_trule
    where rule_code = in_rule_code
    and active_status = 'Y'
    and delete_flag = 'N';

    set v_source_dataset_code = ifnull(v_source_dataset_code,'');
    set v_comparison_dataset_code = ifnull(v_comparison_dataset_code,'');
  end if;

  if in_automatch_flag = 'Y' then
    set v_tran_table = 'recon_trn_ttran';
    set v_tranbrkp_table = 'recon_trn_ttran';
  else
    set v_tran_table = 'recon_tmp_ttran';
    set v_tranbrkp_table = 'recon_tmp_ttran';
  end if;

  -- process
  process_block:begin
    declare process_done int default 0;
    declare process_cursor cursor for
      select
        update_recon_field,
        lookup_recon_field,
        process_apply_on,
        process_method,
        process_query,
        process_function,
        lookup_dataset_code,
        lookup_dataset_field,
        lookup_return_field
      from recon_mst_trulepreprocess
      where recon_code = in_recon_code
      and rule_code = in_rule_code
      and active_status = 'Y'
      and delete_flag = 'N'
      order by process_order;
    declare continue handler for not found set process_done=1;

    open process_cursor;

    process_loop: loop
      fetch process_cursor into
        v_update_recon_field,
        v_lookup_recon_field,
        v_process_apply_on,
        v_process_method,
        v_process_query,
        v_process_function,
        v_lookup_dataset_code,
        v_lookup_dataset_field,
        v_lookup_return_field;

      if process_done = 1 then leave process_loop; end if;

      set v_update_recon_field = ifnull(v_update_recon_field,'');
      set v_lookup_recon_field = ifnull(v_lookup_recon_field,'');
      set v_process_apply_on = ifnull(v_process_apply_on,'');
      set v_process_method = ifnull(v_process_method,'');
      set v_process_query = ifnull(v_process_query,'');
      set v_process_function = ifnull(v_process_function,'');
      set v_lookup_dataset_code = ifnull(v_lookup_dataset_code,'');
      set v_lookup_dataset_field = ifnull(v_lookup_dataset_field,'');
      set v_lookup_return_field = ifnull(v_lookup_return_field,'');

      if v_process_apply_on = 'S' then
        set v_dataset_code = v_source_dataset_code;
      elseif v_process_apply_on = 'C' then
        set v_dataset_code = v_comparison_dataset_code;
      else
        set v_dataset_code = '';
      end if;

      if v_process_method = 'F' then
        set v_sql = 'update $TABLENAME$ set ';
        set v_sql = concat(v_sql,v_update_recon_field,' = ifnull(');
        set v_sql = concat(v_sql,replace(v_process_function,'$FIELD$',v_lookup_recon_field),',',v_update_recon_field,') ');
        set v_sql = concat(v_sql,'where recon_code = ',char(39),in_recon_code,char(39),' ');
        set v_sql = concat(v_sql,'and dataset_code = ',char(39),v_dataset_code,char(39),' ');
        set v_sql = concat(v_sql,v_recon_date_condition);
        set v_sql = concat(v_sql,'and delete_flag = ',char(39),'N',char(39),' ');

        call pr_run_sql(replace(v_sql,'$TABLENAME$',v_tran_table),@msg,@result);
        call pr_run_sql(replace(v_sql,'$TABLENAME$',v_tranbrkp_table),@msg,@result);
      elseif v_process_method = 'Q' then
        set v_sql = v_process_query;
        set v_sql = concat(v_sql,v_recon_date_condition);

        call pr_run_sql(replace(v_sql,'$TABLENAME$',v_tran_table),@msg,@result);
        call pr_run_sql(replace(v_sql,'$TABLENAME$',v_tranbrkp_table),@msg,@result);
      elseif v_process_method = 'L' then
        if v_recon_date_flag = 'Y' then
          set v_recon_date_condition = concat(v_recon_date_condition,' and a.',v_recon_date_field,' >= ');
          set v_recon_date_condition = concat(v_recon_date_condition,char(39),date_format(in_period_from,'%Y-%m-%d'),char(39),' ');
          set v_recon_date_condition = concat(v_recon_date_condition,' and a.',v_recon_date_field,' <= ');
          set v_recon_date_condition = concat(v_recon_date_condition,char(39),date_format(in_period_to,'%Y-%m-%d'),char(39),' ');
        end if;

        set v_sql = 'update $TABLENAME$ as a ';
        set v_sql = concat(v_sql,'inner join ',v_lookup_dataset_code,' as b ');
        set v_sql = concat(v_sql,'on a.',v_lookup_recon_field,'=b.',v_lookup_dataset_field,' ');
        set v_sql = concat(v_sql,'set a.',v_update_recon_field,'=b.',v_lookup_return_field);
        set v_sql = concat(v_sql,'where a.recon_code = ',char(39),in_recon_code,char(39),' ');
        set v_sql = concat(v_sql,'and a.dataset_code = ',char(39),v_dataset_code,char(39),' ');
        set v_sql = concat(v_sql,v_recon_date_condition);
        set v_sql = concat(v_sql,'and a.delete_flag = ',char(39),'N',char(39),' ');

        call pr_run_sql(replace(v_sql,'$TABLENAME$',v_tran_table),@msg,@result);
        call pr_run_sql(replace(v_sql,'$TABLENAME$',v_tranbrkp_table),@msg,@result);
      end if;
    end loop process_loop;

    close process_cursor;
  end process_block;

  set out_result = 1;
  set out_msg = 'Success';

end $$

DELIMITER ;