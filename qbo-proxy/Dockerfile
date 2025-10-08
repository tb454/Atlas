FROM nginx:alpine

# keep image up to date and remove default site
RUN apk update && apk upgrade --no-cache \
  && rm -f /etc/nginx/conf.d/default.conf

# add the nginx template (we'll render it at runtime)
COPY default.conf.template /etc/nginx/templates/default.conf.template

# render the template with env vars, then start nginx in foreground
CMD ["/bin/sh", "-c", "envsubst < /etc/nginx/templates/default.conf.template > /etc/nginx/conf.d/default.conf && exec nginx -g 'daemon off;'"]
