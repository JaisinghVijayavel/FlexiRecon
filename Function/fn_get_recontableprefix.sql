DELIMITER $$

DROP function IF EXISTS `fn_get_recontableprefix` $$
CREATE function `fn_get_recontableprefix`
(
  in_archival_code text,
  in_recon_code text
) returns text
begin
  declare v_archival_db_name text default '';
  declare v_archival_db_prefix text default '';
  declare v_archival_db_flag text default '';

  declare v_concurrent_flag text default '';
  declare v_table_prefix text default '';

  set in_archival_code = ifnull(in_archival_code,'');

  if in_archival_code <> '' then
    set v_archival_db_flag = fn_get_configvalue('archival_db_flag');

    -- chk each archival is separate db
    if v_archival_db_flag = 'Y' then
      -- chk archival db name from config table
      set v_archival_db_name = fn_get_configvalue('archival_db_name');

      if v_archival_db_name = '' then
        -- get archival db name from archival table
        select
          archival_db_name into v_archival_db_name
        from recon_trn_treconarchival
        where archival_code = in_archival_code
        and recon_code = in_recon_code
        and delete_flag = 'N';

        set v_archival_db_name = ifnull(v_archival_db_name,'');
      end if;

      if v_archival_db_name <> '' then
        set v_archival_db_name = concat(v_archival_db_name,'.');
      end if;

      set v_table_prefix = concat(v_archival_db_name,in_archival_code,'_',in_recon_code,'_');
    end if;
  else
    -- concurrent ko flag
    set v_concurrent_flag = fn_get_configvalue('concurrent_ko_flag');

    if v_concurrent_flag = 'Y' then
      set v_table_prefix = concat(in_recon_code,'_');
    else
      set v_table_prefix = 'recon_trn_t';
    end if;
  end if;

  return v_table_prefix;
end $$

DELIMITER ;