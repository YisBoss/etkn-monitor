FROM python:3.12-alpine
WORKDIR /app
# v2.8.10：hosts 公版化——容器内直接 SSH 路由器（连接信息全从设置读），需要 sshpass
RUN apk add --no-cache sshpass
COPY monitor.py .
COPY static/ static/
ENV PYTHONUNBUFFERED=1
EXPOSE 8620
CMD ["python", "-u", "monitor.py"]
