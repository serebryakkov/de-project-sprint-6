WITH user_group_log AS (
	SELECT
		hg.hk_group_id,
		COUNT(DISTINCT luga.hk_user_id) AS cnt_added_users
	FROM SEREBRIAKKOVYANDEXUA__DWH.h_groups hg 
	LEFT JOIN SEREBRIAKKOVYANDEXUA__DWH.l_user_group_activity luga ON hg.hk_group_id = luga.hk_group_id 
	INNER JOIN SEREBRIAKKOVYANDEXUA__DWH.s_auth_history sah ON luga.hk_l_user_group_activity = sah.hk_l_user_group_activity 
	WHERE sah.event = 'add'
	GROUP BY hg.hk_group_id, hg.registration_dt
	ORDER BY hg.registration_dt DESC
	LIMIT 10
),
user_group_messages AS (
	SELECT 
		hg.hk_group_id,
		COUNT(DISTINCT lum.hk_user_id) AS cnt_users_in_group_with_messages
	FROM SEREBRIAKKOVYANDEXUA__DWH.h_groups hg 
	LEFT JOIN SEREBRIAKKOVYANDEXUA__DWH.l_groups_dialogs lgd ON hg.hk_group_id = lgd.hk_group_id 
    INNER JOIN SEREBRIAKKOVYANDEXUA__DWH.l_user_message lum ON lgd.hk_message_id = lum.hk_message_id 
    GROUP BY hg.hk_group_id 
)
SELECT  
	ugl.hk_group_id,
	ugl.cnt_added_users,
	ugm.cnt_users_in_group_with_messages,
	ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users AS group_conversion
FROM user_group_log AS ugl
LEFT JOIN user_group_messages AS ugm ON ugl.hk_group_id = ugm.hk_group_id
ORDER BY ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users DESC;