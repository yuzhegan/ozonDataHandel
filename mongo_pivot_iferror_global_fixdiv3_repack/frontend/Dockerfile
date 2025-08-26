FROM nginx:alpine
ENV API_BASE=http://localhost:8000
COPY index.html /usr/share/nginx/html/index.html
COPY styles.css /usr/share/nginx/html/styles.css
COPY app.js /usr/share/nginx/html/app.js
COPY config.template.js /etc/nginx/templates/config.template.js
COPY entrypoint.sh /docker-entrypoint.d/40-gen-config.sh
RUN chmod +x /docker-entrypoint.d/40-gen-config.sh
EXPOSE 80
