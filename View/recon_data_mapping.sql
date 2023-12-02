create view recon_data_mapping as
select
  a.reconfield_gid,
  a.recon_code,
  b.recon_name,
  a.recon_field_name,
  c.dataset_field_name as field_name,
  d.dataset_name,
  a.display_order,
  c.dataset_code
from recon_mst_treconfield as a
left join recon_mst_trecon as b on a.recon_code = b.recon_code
  and b.delete_flag = 'N'
left join recon_mst_treconfieldmapping as c on a.recon_code = c.recon_code
  and a.recon_field_name = c.recon_field_name
  and c.delete_flag = 'N'
left join recon_mst_tdataset as d on c.dataset_code = d.dataset_code
  and d.delete_flag = 'N'