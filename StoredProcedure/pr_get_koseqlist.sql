DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_koseqlist` $$
CREATE PROCEDURE `pr_get_koseqlist`
(
  in in_recon_code varchar(32),
  in in_user_code varchar(32)
)
BEGIN
  select
	  koseq_gid as koseq_gid,
	  recon_code,
    recon_version,
	  koseq_no as ko_seqno,
	  koseq_type as ko_seq_type,
    fn_get_mastername(koseq_type, 'QCD_ko_seqtype') as ko_seq_type_desc,
	  koseq_ref_code as ko_seq_ref_code,
	  case koseq_type
		  when 'R' then (select rule_name from recon_mst_trule where rule_code = koseq_ref_code)
		  when 'T' then (select theme_name from recon_mst_ttheme where theme_code = koseq_ref_code)
		  when 'P' then (select preprocess_desc from recon_mst_tpreprocess where preprocess_code = koseq_ref_code)
	    else ''
    end AS ko_seq_name,
	  hold_flag,
	  case hold_flag
      when 'Y' then 'Yes'
      else 'No'
    end as hold_flag_desc,
	  active_status,
    fn_get_mastername(active_status, 'QCD_STATUS') as active_status_desc
  from recon_mst_tkoseq
  where recon_code = in_recon_code
  and active_status = 'Y'
  and delete_flag = 'N';
END $$

DELIMITER ;