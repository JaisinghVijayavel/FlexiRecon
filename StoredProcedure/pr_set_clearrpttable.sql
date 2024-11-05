DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_clearrpttable` $$
CREATE PROCEDURE `pr_set_clearrpttable`()
me:BEGIN
	truncate recon_trn_tpreview;
	truncate recon_trn_tpreviewdtl;
	truncate con_trn_tbcp;
	truncate recon_rpt_tko;
	truncate recon_rpt_ttran;
	truncate recon_rpt_ttranbrkp;
	truncate recon_rpt_ttranwithbrkp;
	truncate recon_rpt_tmanualpost;
	truncate recon_rpt_tmanualmatch;
	truncate recon_rpt_tpreview;
	truncate recon_rpt_tdataset;
end $$

DELIMITER ;