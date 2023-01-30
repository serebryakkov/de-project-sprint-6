INSERT INTO SEREBRIAKKOVYANDEXUA__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
SELECT DISTINCT
	hash(gl.group_id, gl.user_id) AS hk_l_user_group_activity,
	hu.hk_user_id,
	hg.hk_group_id,
	now() AS load_dt,
	'S3' AS load_src
FROM SEREBRIAKKOVYANDEXUA__STAGING.group_log AS gl
LEFT JOIN SEREBRIAKKOVYANDEXUA__DWH.h_users AS hu ON gl.user_id = hu.user_id 
LEFT JOIN SEREBRIAKKOVYANDEXUA__DWH.h_groups AS hg ON gl.group_id = hg.group_id 
;

INSERT INTO SEREBRIAKKOVYANDEXUA__DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)
SELECT
	luga.hk_l_user_group_activity,
	gl.user_id_from,
	gl.event,
	gl.datetime AS event_dt,
	now() AS load_dt,
	'S3' AS load_src
FROM SEREBRIAKKOVYANDEXUA__STAGING.group_log AS gl
LEFT JOIN SEREBRIAKKOVYANDEXUA__DWH.h_groups AS hg ON gl.group_id = hg.group_id
LEFT JOIN SEREBRIAKKOVYANDEXUA__DWH.h_users AS hu ON gl.user_id = hu.user_id
LEFT JOIN SEREBRIAKKOVYANDEXUA__DWH.l_user_group_activity AS luga ON hg.hk_group_id = luga.hk_group_id AND hu.hk_user_id = luga.hk_user_id
; 