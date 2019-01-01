# Apache Beam Testing
A collection of sample Python scripts using [Apache Beam](https://beam.apache.org/).

## Get Set Up

1. Install Python with [Anaconda](https://www.anaconda.com/download/)
2. Create and activate a new Conda 2.7 environment (Apache Beam only supports Python 2.7)
3. [Clone the Github repo](https://github.com/rdempsey/apache_beam_testing) and cd into the directory
4. Install the required Python libraries: `pip install -r requirements.txt`
5. Run the script of your choice!

## Run the Tests

Run the unit tests in the root directory:

```
python -m unittest discover -s tests -t tests
```

## Check the Style

Want to be sure I'm following the Python style guide? Run this command:

```
flake8 scripts
```

## Scripts

* `hello_beam.py`: creates a file from another file.
* `wordcount_minimal.py`: a minimalist word-counting workflow that counts words in a file.
* `streaming_wordcount.py`: a streaming word-counting workflow.