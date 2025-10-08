FROM nginx:alpine
RUN apk update && apk upgrade --no-cache \
	&& rm -f /etc/nginx/conf.d/default.conf
COPY default.conf.template /etc/nginx/templates/default.conf.template
