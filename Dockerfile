FROM python:3.8

WORKDIR /smobem
COPY ./requirements.txt /smobem/requirements.txt
RUN pip install -r requirements.txt

COPY ./core/ /smobem/core
COPY ./docs/ /smobem/docs
COPY ./tests/ /smobem/tests
EXPOSE 8002
CMD ["uvicorn", "core.main_api:app", "--reload",  "--host", "0.0.0.0", "--port", "8002"]
# "--reload",
# docker build -t smabem-api .
# docker run -d --name smobem -p 8002:8002 smabem-api
