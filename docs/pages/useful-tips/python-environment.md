---
title: Python Environment
order: 0
status: 'published'
---

# Python Environment

By default, Fennel server runs on Python 3.11 and comes pre-installed with the
following pip packages:

```
charset-normalizer==3.0.1
cloudpickle==2.2.0
frozenlist==1.3.3 
great_expectations"[pandas,postgresql]"==0.15.49
idna==3.4 
jmespath==1.0.1 
mpmath==1.2.1 
multidict==6.0.4 
nltk==3.8.1 
numpy==1.24.4
pandas[performance]==2.2.2
pyarrow==14.0.0 
pyparsing==3.0.9 
pyspark==3.4.0
python-dateutil==2.8.2
pytz==2022.7.1 
regex==2022.10.31 
requests==2.28.2
scikit-learn==1.2.0 
scipy==1.10.0 
six==1.16.0 
statsmodels==0.13.5 
sympy==1.11.1 
typing-extensions==4.4.0 
urllib3==1.26.14
xgboost==1.7.3
yarl==1.8.2
```

If you want to run on some other Python version and/or install any specific pip 
packages with specific versions, please contact Fennel support.

In the near future, Fennel will also allow you to configure your environment on 
your own using Fennel console without dependency on Fennel support..