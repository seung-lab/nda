CREATE TABLE `channel` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8;

CREATE TABLE `voxel_set` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `boss_vset_id` bigint(20) unsigned NOT NULL,
  `size` bigint(20) unsigned DEFAULT NULL,
  `key_point_x` int(10) NOT NULL,
  `key_point_y` int(10) NOT NULL,
  `key_point_z` int(10) NOT NULL,
  `x_min` int(10) NOT NULL,
  `y_min` int(10) NOT NULL,
  `z_min` int(10) NOT NULL,
  `x_max` int(10) NOT NULL,
  `y_max` int(10) NOT NULL,
  `z_max` int(10) NOT NULL,
  `channel` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `boss_vset_id` (`boss_vset_id`,`channel`),
  KEY `key_point_x_idx` (`key_point_x`),
  KEY `key_point_y_idx` (`key_point_y`),
  KEY `key_point_z_idx` (`key_point_z`),
  KEY `x_min_idx` (`x_min`),
  KEY `y_min_idx` (`y_min`),
  KEY `z_min_idx` (`z_min`),
  KEY `x_max_idx` (`x_max`),
  KEY `y_max_idx` (`y_max`),
  KEY `z_max_idx` (`z_max`),
  KEY `fk_channel` (`channel`),
  CONSTRAINT `fk_channel` FOREIGN KEY (`channel`) REFERENCES `channel` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2664646 DEFAULT CHARSET=utf8;

CREATE TABLE `neuron` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `voxel_set` bigint(20) unsigned DEFAULT NULL,
  `em_id` smallint(6) DEFAULT NULL COMMENT 'em functional id',
  PRIMARY KEY (`id`),
  KEY `voxel_set_idx` (`voxel_set`),
  CONSTRAINT `fk_voxel_set` FOREIGN KEY (`voxel_set`) REFERENCES `voxel_set` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2204017 DEFAULT CHARSET=utf8;

CREATE TABLE `synapse` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `voxel_set` bigint(20) unsigned DEFAULT NULL,
  `pre` bigint(20) unsigned DEFAULT NULL,
  `post` bigint(20) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `pre_idx` (`pre`),
  KEY `post_idx` (`post`),
  KEY `voxel_set_idx` (`voxel_set`),
  CONSTRAINT `fk_synapse_post_neuron_id` FOREIGN KEY (`post`) REFERENCES `neuron` (`id`),
  CONSTRAINT `fk_synapse_pre_neuron_id` FOREIGN KEY (`pre`) REFERENCES `neuron` (`id`),
  CONSTRAINT `fk_synapse_voxel_set_id` FOREIGN KEY (`voxel_set`) REFERENCES `voxel_set` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=79255 DEFAULT CHARSET=utf8;
