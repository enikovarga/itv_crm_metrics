# itv_crm_metrics

## Running the application and tests

### Prerequisites

**Python 3.11.\* or later.**

Check if python3 is installed:

```
python3 --version
```


### Dependencies and data

#### Creating a virtual environment
Ensure the pip package manager is up to date:

```
pip3 install --upgrade pip
```

From the root directory of the project, create and activate the virtual environment:

```
python3 -m venv ./venv
source ./venv/bin/activate
```


#### Installing Python requirements
This will install the packages required for the application to run:

```
pip3 install -r ./requirements.txt
```


### Execution

#### Running the application
Running the application to generate the desired output
```
python3 src/main.py
```


#### Running the tests
Running all unit tests to validate the application functionality
```
pytest -vvv tests
```


### Clean-up
Post-execution clean-up to remove data and virtual environment generated during the execution process
```
rm -r venv
```


### Author
[Eniko Varga](eniko.varga@live.com)
