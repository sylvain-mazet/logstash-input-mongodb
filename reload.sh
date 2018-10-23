#!/bin/bash

jruby='/data/smazet/jruby/jruby-9.1.17.0/bin/jruby'

$jruby -S gem build logstash-input-mongodb
#jruby -S bundle install
sudo -u logstash /usr/share/logstash/bin/logstash-plugin install --no-verify logstash-input-mongodb-0.4.2.gem 
