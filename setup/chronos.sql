CREATE DATABASE IF NOT EXISTS chronos;
USE chronos;
DROP TABLE IF EXISTS `tasks`;

CREATE TABLE `tasks` (
    `id` binary(16) NOT NULL,
    `plugin` varchar(30) NOT NULL,
    `lock_type` varchar(50),
    `status` enum('queued', 'locked', 'running', 'complete', 'error', 'timed_out') NOT NULL,
    `locked` bool DEFAULT 0,
    `modified` timestamp DEFAULT current_timestamp ON UPDATE current_timestamp,
    `created` timestamp NOT NULL,
    `locked_at` timestamp,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB;
