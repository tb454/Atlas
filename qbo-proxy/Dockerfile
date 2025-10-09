FROM nginx:alpine

RUN apk add --no-cache gettext \
  && rm -f /etc/nginx/conf.d/default.conf

COPY default.conf.template /etc/nginx/templates/default.conf.template

# ⬇️ Only substitute ${QBO_RELAY_AUTH}; leave $binary_remote_addr intact
CMD ["/bin/sh", "-c", "envsubst '${QBO_RELAY_AUTH}' < /etc/nginx/templates/default.conf.template > /etc/nginx/conf.d/default.conf && exec nginx -g 'daemon off;'"]
