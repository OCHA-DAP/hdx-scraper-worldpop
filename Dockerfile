FROM public.ecr.aws/unocha/python:3.12-stable

WORKDIR /srv

COPY . .

RUN pip install --no-cache-dir .

CMD "python3 -m hdx.scraper.worldpop"
