FROM public.ecr.aws/unocha/python:3.12-stable

WORKDIR /srv

COPY . .

RUN pip --no-cache-dir install --upgrade -r requirements.txt && \
    pip --no-cache-dir install .

CMD ["python3", "-m", "hdx.scraper.worldpop"]
