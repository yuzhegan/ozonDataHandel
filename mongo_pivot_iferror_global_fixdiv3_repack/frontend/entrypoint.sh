#!/bin/sh
set -e
envsubst '${API_BASE}' < /etc/nginx/templates/config.template.js > /usr/share/nginx/html/config.js
