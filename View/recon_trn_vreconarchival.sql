alter view recon_trn_vreconarchival as
select
  a.reconarchival_gid,a.scheduler_gid,a.archival_code,a.recon_code,b.recon_name,
  a.archival_date,a.archival_by
from recon_trn_treconarchival as a
left join recon_mst_trecon as b on a.recon_code = b.recon_code
  and b.active_status = 'Y'
  and b.delete_flag = 'N'
where a.delete_flag = 'N'