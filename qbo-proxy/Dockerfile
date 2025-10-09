FROM nginx:alpine

# Update all packages to their latest versions to reduce vulnerabilities
RUN apk update && apk upgrade

# install envsubst (from gettext) and remove default site
RUN apk add --no-cache gettext \
  && rm -f /etc/nginx/conf.d/default.conf

# copy the template; we'll render it on startup
COPY default.conf.template /etc/nginx/templates/default.conf.template

# render template with env vars -> live config, then start nginx
CMD ["/bin/sh", "-c", "envsubst < /etc/nginx/templates/default.conf.template > /etc/nginx/conf.d/default.conf && exec nginx -g 'daemon off;'"]
