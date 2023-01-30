DROP TABLE IF EXISTS SEREBRIAKKOVYANDEXUA__STAGING.group_log;

CREATE TABLE SEREBRIAKKOVYANDEXUA__STAGING.group_log (
	group_id int,
	user_id int,
	user_id_from int,
	event varchar(6),
	datetime datetime
);

DROP TABLE IF EXISTS SEREBRIAKKOVYANDEXUA__DWH.l_user_group_activity;

CREATE TABLE SEREBRIAKKOVYANDEXUA__DWH.l_user_group_activity (
	hk_l_user_group_activity int PRIMARY KEY,
	hk_user_id int NOT NULL REFERENCES SEREBRIAKKOVYANDEXUA__DWH.h_users (hk_user_id),
	hk_group_id int NOT NULL REFERENCES SEREBRIAKKOVYANDEXUA__DWH.h_groups (hk_group_id),
	load_dt datetime,
	load_src varchar(20)
);

DROP TABLE IF EXISTS SEREBRIAKKOVYANDEXUA__DWH.s_auth_history;

CREATE TABLE SEREBRIAKKOVYANDEXUA__DWH.s_auth_history (
	hk_l_user_group_activity int NOT NULL REFERENCES SEREBRIAKKOVYANDEXUA__DWH.l_user_group_activity (hk_l_user_group_activity),
	user_id_from int,
	event varchar(6),
	event_dt datetime,
	load_dt datetime,
	load_src varchar(20)
);