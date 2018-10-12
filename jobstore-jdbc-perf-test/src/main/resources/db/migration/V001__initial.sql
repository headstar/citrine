CREATE TABLE `job` (
  `id` varchar(255) NOT NULL,
  `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `job_state` varchar(45) NOT NULL,
  `next_execution_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `job_class` varchar(255) NOT NULL,
  `priority` smallint NOT NULL,
  `job_data` varchar(2048) DEFAULT NULL,
  `cron_expression` varchar(255) DEFAULT NULL,
  `version` int(11) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

ALTER TABLE `job` ADD INDEX `next_execution_time_priority_index` (`priority`, `next_execution_time`);
