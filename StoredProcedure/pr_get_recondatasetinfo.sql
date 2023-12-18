DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_recondatasetinfo` $$
CREATE PROCEDURE `pr_get_recondatasetinfo`(
  in in_recon_code varchar(32),
  in in_dataset_code varchar(32),
  in in_automatch_flag char(1),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_recon_name text default '';
  declare v_recontype_code varchar(32) default '';

  -- get recon info
  select
    recon_name,
    recontype_code
  into
    v_recon_name,
    v_recontype_code
  from recon_mst_trecon
  where recon_code = in_recon_code
  and delete_flag = 'N';

  if v_recontype_code = 'B' or v_recontype_code = 'W' or v_recontype_code = 'I' then
    select
      before_dr_count as 'Dr Count Before',
      after_dr_count as 'Dr Count After',
      before_dr_value as 'Dr Value Before',
      after_dr_value as 'Dr Value After',
      before_cr_count as 'Cr Count Before',
      after_cr_value as 'Cr Value After',
      after_cr_count as 'Cr Count After',
      before_cr_value as 'Cr Value Before',
      before_count as 'Count Before',
      after_count as 'Count After',
      before_value as 'Value Before',
      after_value as 'Value After'
    from recon_trn_tdatasetjob
    where recon_code = in_recon_code
    and dataset_code = in_dataset_code
    and automatch_flag = in_automatch_flag
    and delete_flag = 'N';
  else
    select
      before_count as 'Count Before',
      after_count as 'Count After',
      before_value as 'Value Before',
      after_value as 'Value After'
    from recon_trn_tdatasetjob
    where recon_code = in_recon_code
    and dataset_code = in_dataset_code
    and automatch_flag = in_automatch_flag
    and delete_flag = 'N';
  end if;
end $$

DELIMITER ;