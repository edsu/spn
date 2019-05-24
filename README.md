This repository includes Jupyter notebooks that document research into the
Internet Archive's [Save Page Now] web archive data. The project is a
collaboration between Shawn Walker, Jess Ogden and Ed Summers.

**Notebooks:**

- [Sizes]: an exploration of how the SPN data has grown over time.
- [Sample]: an example of sampling the SPN data over time.
- [Spark]: a demonstration of using Spark with warcio
- [UserAgent]: looking at User Agents archiving at SPN

Note: if you are using a notebook that requires Spark you'll need to set these
in your environment before starting Jupyter:

    export PYSPARK_DRIVER_PYTHON=jupyter
    export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
    jupyter notebook Spark.ipynb

**Utilities:**

- check.py: a utility to ensure that the downloaded files are complete

[Sizes]: https://github.com/edsu/spn/blob/master/notebooks/Sizes.ipynb
[Sample]: https://github.com/edsu/spn/blob/master/notebooks/Sample.ipynb
[Spark]: https://github.com/edsu/spn/blob/master/notebooks/Spark.ipynb
[UserAgent]: https://github.com/edsu/spn/blob/master/notebooks/UserAgent.ipynb
[Save Page Now]: https://wayback.archive.org
