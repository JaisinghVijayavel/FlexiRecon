DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_fetch_reconversion` $$
CREATE PROCEDURE `pr_fetch_reconversion`
(
  in in_recon_code varchar(32),
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32)
)
BEGIN
  -- rule
  select
    rule_gid,
    rule_code,
    b.rule_order,
    rule_name,
    a.recon_code,
    a.recon_name,
    source_dataset_code,
    a.recon_rule_version,
    c.dataset_name as source_dataset_desc,
    comparison_dataset_code,
    d.dataset_name as comparison_dataset_desc,
    a.recon_rule_version,
    system_match_flag,
    case system_match_flag
      when 'Y' then 'YES'
      else 'NO'
    end as system_match_flag_desc,
    hold_flag,
    case hold_flag
      when 'Y' then 'YES'
      else 'NO'
    end as hold_flag_desc,
    probable_match_flag as probable_flag,
    case probable_match_flag
      when 'Y' then 'YES'
      else 'NO'
    end as probable_match_desc
  from recon_mst_trecon a
  inner join recon_mst_trule b on a.recon_code=b.recon_code
    and b.active_status = 'Y' and b.delete_flag = 'N'
  inner join recon_mst_tdataset c on b.source_dataset_code=c.dataset_code
    and c.delete_flag = 'N'
  inner join recon_mst_tdataset d on b.comparison_dataset_code=d.dataset_code
    and d.delete_flag = 'N'
  where a.recon_code=in_recon_code
  and a.delete_flag='N'
  order by b.rule_order;

  -- rule version
  if exists(select recon_version from recon_mst_treconversion
    where recon_code = in_recon_code
    and delete_flag = 'N') then
    select
      distinct recon_version as recon_rule_version
    from recon_mst_treconversion
    where recon_code=in_recon_code
    and delete_flag = 'N'
    order by cast(replace(recon_version,'.','') as unsigned) desc;
  else
    select '0.0.0' as recon_rule_version;
  end if;

  -- theme
  select
    theme_gid,
    theme_code,
    theme_desc,
    a.recon_code,
    theme_order,
    b.recon_name,
    a.active_status,
    fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc,
    a.hold_flag,
    fn_get_mastername(a.hold_flag, 'QCD_YN') as hold_flag_desc
  from recon_mst_ttheme  a
  inner join recon_mst_trecon b on a.recon_code=b.recon_code
    and b.delete_flag = 'N'
  where a.recon_code=in_recon_code
  and a.active_status='Y'
  and a.delete_flag = 'N';

  -- preprocess
  select
    preprocess_gid,
    preprocess_code,
    preprocess_desc,
    preprocess_order,
    a.hold_flag,
    fn_get_mastername(a.hold_flag, 'QCD_YN') as hold_flag_desc
  from recon_mst_tpreprocess  a
  inner join recon_mst_trecon b on a.recon_code=b.recon_code
    and b.delete_flag = 'N'
  where a.recon_code=in_recon_code
  and a.active_status='Y'
  and a.delete_flag = 'N';
END $$

DELIMITER ;