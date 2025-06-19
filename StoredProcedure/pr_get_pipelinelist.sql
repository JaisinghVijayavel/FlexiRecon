DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_pipelinelist` $$
CREATE PROCEDURE `pr_get_pipelinelist`
(
  in in_target_dataset_code varchar(32)
)
BEGIN
  select
    a.pipeline_gid,
    a.pipeline_code,
    a.pipeline_name,
    a.pipeline_desc,
    a.pipeline_status,
    concat('.',substring_index(a.source_file_name, '.', -1)) as file_extenstion
  from con_mst_tpipeline as a
  inner join con_trn_tpipelinedetails as b on a.pipeline_code = b.pipeline_code
    and b.pipelinedet_status = 'Active'
    and b.delete_flag = 'N'
  where true
  and b.target_dataset_code = in_target_dataset_code
  and a.pipeline_status = 'Active'
  and a.delete_flag = 'N';
END $$

DELIMITER ;