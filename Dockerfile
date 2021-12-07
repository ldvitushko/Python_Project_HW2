FROM python:3.8
WORKDIR /Python_Course_HW2
ADD . /Python_Course_HW2
RUN pip install -r requirements.txt
CMD ["python", "app.py"]
