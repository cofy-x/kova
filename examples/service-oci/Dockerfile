FROM $KOVA_IMAGE_REGISTRY/kova-examples/python-smoke-base:dev

LABEL org.opencontainers.image.title="kova python oci smoke"

WORKDIR /app
COPY app.py /app/app.py

EXPOSE 8080
CMD ["python3", "/app/app.py"]
