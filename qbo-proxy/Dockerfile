FROM nginx:alpine

# remove the default site
RUN rm -f /etc/nginx/conf.d/default.conf

# copy the template
COPY default.conf.template /etc/nginx/templates/default.conf.template

# render: only replace the token __RELAY_AUTH__ with $QBO_RELAY_AUTH, then start nginx
CMD ["/bin/sh","-c","sed -e 's|__RELAY_AUTH__|'\"$QBO_RELAY_AUTH\"'|g' /etc/nginx/templates/default.conf.template > /etc/nginx/conf.d/default.conf && exec nginx -g 'daemon off;'"]
