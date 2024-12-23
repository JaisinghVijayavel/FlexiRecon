DELIMITER $$

DROP procedure IF EXISTS `pr_set_pdrefno` $$
CREATE procedure `pr_set_pdrefno`
(
  in_recon_code varchar(32)
)
me:begin
  declare v_sql text default '';

  declare v_tran_table text default '';
  declare v_tranbrkp_table text default '';

  declare v_dsdb_name text default '';
  declare v_themeds_code text default '';
  declare v_unitds_code text default '';

  declare v_lineref_no_field text default '';
  declare v_themeref_no_field text default '';
  declare v_entrypass_field text default '';
  declare v_entryref_no_field text default '';
  declare v_action_field text default '';


  set v_tran_table = 'recon_trn_ttran';
  set v_tranbrkp_table = 'recon_trn_ttranbrkp';

  -- set line ref no
  -- find line ref no column
  set v_lineref_no_field = fn_get_reconfieldfromdesc(in_recon_code,'Line Ref No');

  if v_lineref_no_field <> '' then
    -- set line ref no
    -- tran table Ex:T1
    set v_sql = concat("update ",v_tran_table," set
            ",v_lineref_no_field," = concat('T',cast(tran_gid as nchar))
        where recon_code = '",in_recon_code,"'
        and delete_flag = 'N'");

    call pr_run_sql2(v_sql,@msg,@result);

    -- tranbrkp table Ex:T1
    set v_sql = concat("update ",v_tranbrkp_table," set
            ",v_lineref_no_field," = concat('S',cast(tranbrkp_gid as nchar))
        where recon_code = '",in_recon_code,"'
        and delete_flag = 'N'");

    call pr_run_sql2(v_sql,@msg,@result);
  end if;

  -- find theme master dataset

  -- col1 - Theme Code
  -- col2 - Theme Description Reference
  -- col3 - Theme Description Like Condition
  -- col4 - Theme Description Reference
  select
    dataset_code into v_themeds_code
  from recon_mst_tdataset
  where dataset_name = 'Theme Master'
  and active_status = 'Y'
  and delete_flag = 'N';

  set v_themeds_code = ifnull(v_themeds_code,'');

  -- find unit master dataset

  -- col1 - Unit Code
  -- col2 - Unit Name
  -- col3 - Unit Location Code
  -- col4 - Recon Code
  -- col5 - Cluster
  -- col6 - Unit Location Name
  select
    dataset_code into v_unitds_code
  from recon_mst_tdataset
  where dataset_name = 'Unit Master'
  and active_status = 'Y'
  and delete_flag = 'N';

  set v_unitds_code = ifnull(v_unitds_code,'');

  if v_themeds_code <> '' and v_unitds_code <> '' then
    -- get dataset db name
    select fn_get_configvalue('dataset_db_name') into v_dsdb_name;

    set v_dsdb_name = ifnull(v_dsdb_name,'');

    if v_dsdb_name <> '' then
      set v_themeds_code = concat(v_dsdb_name,'.',v_themeds_code);
      set v_unitds_code = concat(v_dsdb_name,'.',v_unitds_code);
    end if;

    -- set theme ref no
    -- find theme ref no column
    set v_themeref_no_field = fn_get_reconfieldfromdesc(in_recon_code,'Theme Ref No');

    if v_themeref_no_field <> '' then
      -- update theme ref no
      set v_sql = concat("update ",v_tran_table," as a
        inner join ",v_themeds_code," as b on a.theme_code like b.col3
          and b.delete_flag = 'N'
        inner join ",v_unitds_code," as c on a.recon_code = c.col4
          and c.delete_flag = 'N'
        set a.",v_themeref_no_field," = concat('TH',b.col1,'_',c.col1,'_',date_format(curdate(),'%d%m%y'))
        where a.recon_code = '",in_recon_code,"'
        and a.delete_flag = 'N'");


      call pr_run_sql2(v_sql,@msg,@result);

      set v_sql = concat("update ",v_tranbrkp_table," as a
        inner join ",v_themeds_code," as b on a.theme_code like b.col3
          and b.delete_flag = 'N'
        inner join ",v_unitds_code," as c on a.recon_code = c.col4
          and c.delete_flag = 'N'
        set a.",v_themeref_no_field," = concat('TH',b.col1,'_',c.col1,'_',date_format(curdate(),'%d%m%y'))
        where a.recon_code = '",in_recon_code,"'
        and a.delete_flag = 'N'");

      call pr_run_sql2(v_sql,@msg,@result);
    end if;

    -- find entry pass and ref no field
    set v_entrypass_field = fn_get_reconfieldfromdesc(in_recon_code,'Entry Pass Flag');
    set v_entryref_no_field = fn_get_reconfieldfromdesc(in_recon_code,'Entry Ref No');

    if v_entrypass_field <> '' and v_entryref_no_field <> '' then
      -- set entry pass flag 'Y'
      -- update entry pass flag
      set v_sql = concat("update ",v_tran_table," as a
        inner join ",v_themeds_code," as b on a.theme_code like b.col3
          and b.delete_flag = 'N'
        inner join ",v_unitds_code," as c on a.recon_code = c.col4
          and c.delete_flag = 'N'
        set a.",v_entrypass_field," = 'Y'
        where a.recon_code = '",in_recon_code,"'
        and a.",v_entrypass_field," is null
        and a.delete_flag = 'N'");

      call pr_run_sql2(v_sql,@msg,@result);

      set v_sql = concat("update ",v_tranbrkp_table," as a
        inner join ",v_themeds_code," as b on a.theme_code like b.col3
          and b.delete_flag = 'N'
        inner join ",v_unitds_code," as c on a.recon_code = c.col4
          and c.delete_flag = 'N'
        set a.",v_entrypass_field," = 'Y'
        where a.recon_code = '",in_recon_code,"'
        and a.",v_entrypass_field," is null
        and a.delete_flag = 'N'");

      call pr_run_sql2(v_sql,@msg,@result);

      -- update entry ref no
      set v_sql = concat("update ",v_tran_table," as a
        inner join ",v_themeds_code," as b on a.theme_code like b.col3
          and b.delete_flag = 'N'
        inner join ",v_unitds_code," as c on a.recon_code = c.col4
          and c.delete_flag = 'N'
        set a.",v_entryref_no_field," = concat('PD',b.col1,'_',c.col1,'_',date_format(curdate(),'%d%m%y'))
        where a.recon_code = '",in_recon_code,"'
        and a.",v_entrypass_field," = 'Y'
        and a.delete_flag = 'N'");

      call pr_run_sql2(v_sql,@msg,@result);

      set v_sql = concat("update ",v_tranbrkp_table," as a
        inner join ",v_themeds_code," as b on a.theme_code like b.col3
          and b.delete_flag = 'N'
        inner join ",v_unitds_code," as c on a.recon_code = c.col4
          and c.delete_flag = 'N'
        set a.",v_entryref_no_field," = concat('PD',b.col1,'_',c.col1,'_',date_format(curdate(),'%d%m%y'))
        where a.recon_code = '",in_recon_code,"'
        and a.",v_entrypass_field," = 'Y'
        and a.delete_flag = 'N'");

      call pr_run_sql2(v_sql,@msg,@result);
    end if;

    -- set action field
    -- find line ref no column
    set v_action_field = fn_get_reconfieldfromdesc(in_recon_code,'Action To Be Taken');

    if v_action_field <> '' then
      -- update action to be taken
      set v_sql = concat("update ",v_tran_table," as a
        inner join ",v_themeds_code," as b on a.theme_code like b.col3
          and b.delete_flag = 'N'
        inner join ",v_unitds_code," as c on a.recon_code = c.col4
          and c.delete_flag = 'N'
        set a.",v_action_field," = concat(b.col1,'-',b.col4)
        where a.recon_code = '",in_recon_code,"'
        and a.delete_flag = 'N'");


      call pr_run_sql2(v_sql,@msg,@result);

      set v_sql = concat("update ",v_tranbrkp_table," as a
        inner join ",v_themeds_code," as b on a.theme_code like b.col3
          and b.delete_flag = 'N'
        inner join ",v_unitds_code," as c on a.recon_code = c.col4
          and c.delete_flag = 'N'
        set a.",v_action_field," = concat(b.col1,'-',b.col4)
        where a.recon_code = '",in_recon_code,"'
        and a.delete_flag = 'N'");

      call pr_run_sql2(v_sql,@msg,@result);
    end if;
  end if;
end $$

DELIMITER ;