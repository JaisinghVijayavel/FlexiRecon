DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_koqueue` $$
CREATE PROCEDURE `pr_get_koqueue`
(
  in in_start_date date,
  in in_end_date date,
  in in_jobtype_code char(1),
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32)
)
me:BEGIN
  /*
    Created By :
    Created Date :

    Updated By : Vijayavel
    Updated Date : 05-05-2026

    Version : 1
  */

  declare v_app_datetime_format text default '';

  set v_app_datetime_format = fn_get_configvalue('app_datetime_format');

  select
			a.koqueue_gid,
			a.recon_code,
			d.recon_name,
			date_format(a.scheduled_date,'%d-%m-%Y %H:%i:%s') as scheduled_date,
			a.koqueue_status,
			a.koqueue_remark,
      case when a.koqueue_status='I' then 'Scheduled' else 'Cancelled' end as jobstatus_desc,
      a.scheduled_by,
      ifnull(if(a.queue_type = 'R','Report','Knock Off'),'Knock Off') as queue_type,
      if(a.queue_type = 'R',fn_get_report_template_name(a.koqueue_gid),'Knock Off') as queue_name
  from recon_trn_tkoqueue a
  inner join recon_mst_tjobstatus b on a.koqueue_status = b.job_status
  left join recon_mst_trecon d on a.recon_code = d.recon_code
  where 1 = 1
  and a.koqueue_status IN ('I')
  and a.delete_flag = 'N'
  order by a.job_gid desc;
 
END $$

DELIMITER ;