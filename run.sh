#!/bin/bash

sudo rm /etc/logstash/states/logstash_dev_v4_franck.db
debug='--log.level debug'
sudo -u logstash /usr/share/logstash/bin/logstash $debug  -f /home/smazet/scripts/context_1_connector_5.conf --path.settings /etc/logstash/ --path.data /tmp
