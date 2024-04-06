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
  select
    rule_gid,
    rule_code,
    rule_name,
    a.recon_code,
    a.recon_name,
    source_dataset_code,
    a.recon_rule_version,
    c.dataset_name as source_dataset_desc,
    comparison_dataset_code,
    d.dataset_name as comparison_dataset_desc,
    a.recon_rule_version,system_match_flag,
    case system_match_flag
      when 'Y' then 'YES'
      else 'NO'
    end as system_match_flag_desc,
    hold_flag,
    case hold_flag
      when 'Y' then 'YES'
      else 'NO'
    end as hold_flag_desc
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

  select distinct recon_rule_version from recon_mst_trulehistory
  where recon_code=in_recon_code
  and delete_flag = 'N'
  order by cast(replace(recon_rule_version,'.','') as unsigned) desc;
END $$

DELIMITER ;