FROM python:3
RUN mkdir -p /usr/src/processing
WORKDIR /usr/src/processing
RUN pip install tqdm numpy pandas
COPY . .
CMD tail -f /dev/null
# CMD ["pip", "freeze"]