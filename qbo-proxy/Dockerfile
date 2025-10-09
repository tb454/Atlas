FROM nginx:alpine
RUN rm -f /etc/nginx/conf.d/default.conf
COPY default.conf.template /etc/nginx/templates/default.conf.template
CMD ["/bin/sh","-c","sed -e 's|__RELAY_AUTH__|'\"$QBO_RELAY_AUTH\"'|g' /etc/nginx/templates/default.conf.template > /etc/nginx/conf.d/default.conf && exec nginx -g 'daemon off;'"]
