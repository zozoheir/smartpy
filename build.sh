#!/bin/bash

aws s3 cp s3://dqc-testing/dqcpy-0.1.0-py3-none-any.whl .
aws s3 cp s3://dqc-testing/requirements.txt .
pip install dqcpy-0.1.0-py3-none-any.whl
pip install -r requirements.txt
python -m ipykernel install --user --name=dqcpy
