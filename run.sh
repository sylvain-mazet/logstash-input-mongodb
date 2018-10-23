#!/bin/bash

# --log.level debug
sudo -u logstash /usr/share/logstash/bin/logstash -f /tmp/context_1_connector_5.conf --path.settings /etc/logstash/ --path.data /tmp
