FROM python:3.9.12-slim
RUN mkdir /usr/app
WORKDIR /usr/app
COPY . .
RUN yarn
ENV PATH /usr/app/node_modules/.bin:$PATH
RUN npm run build

#For nginx
FROM nginx:stable-alpine
COPY --from=build /usr/app/build /usr/share/nginx/html/
RUN rm /etc/nginx/conf.d/default.conf
COPY nginx/nginx.conf /etc/nginx/conf.d

#Run nginx
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]