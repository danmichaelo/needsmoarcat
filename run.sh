#!/bin/bash

. ../ENV/bin/activate
rm -f *.cache
python run.py | tee out.log

