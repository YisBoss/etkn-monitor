FROM python:3.12-alpine
WORKDIR /app
COPY monitor.py .
COPY static/ static/
ENV PYTHONUNBUFFERED=1
EXPOSE 8620
CMD ["python", "-u", "monitor.py"]
