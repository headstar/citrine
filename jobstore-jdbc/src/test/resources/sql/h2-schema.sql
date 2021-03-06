CREATE TABLE `job` (
  `id` varchar(255) NOT NULL,
  `created_time` timestamp NOT NULL,
  `updated_time` timestamp NOT NULL,
  `job_state` varchar(45) NOT NULL,
  `next_execution_time` timestamp NULL DEFAULT '0000-00-00 00:00:00',
  `job_class` varchar(255) NOT NULL,
  `priority` smallint NOT NULL,
  `job_data` varchar(2048) DEFAULT NULL,
  `cron_expression` varchar(255) DEFAULT NULL,
  `version` int(11) NOT NULL,
  PRIMARY KEY (`id`)
);

