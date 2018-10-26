#!/bin/bash

sudo rm -f /etc/logstash/states/softbridge_tests_v4.db
debug='--log.level debug'
file=/tmp/context_1_connector_3.conf
sudo -u logstash /usr/share/logstash/bin/logstash $debug  -f $file --path.settings /etc/logstash/ --path.data /tmp
