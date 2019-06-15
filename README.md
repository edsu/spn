This repository includes Jupyter notebooks that document research into the
Internet Archive's [Save Page Now] web archive data. The project is a
collaboration between Shawn Walker, Jess Ogden and Ed Summers.

**Notebooks:**

The notebooks do have some order to them since some of them rely on data created
in others. They are listed here as a table of contents if you want to
follow the path of exploration.

- [Sizes]: how SPN data has changed over time
- [Sample]: sampling the full SPN dataset
- [Spark]: an example of using Spark with WARC data
- [Tracery]: tracing SPN requests in WARC data
- [URLs]: extracting metadata for SPN requests
- [UserAgents]: analyzing the User-Agents in SPN requests
- [Domains]: examining the most popularly archived domains
- [Archival Novelty]: what does newness look like in SPN data
- [WSDL Diversity Index]: analyzing the diversity of SPN requests
- [Known Sites]: taking a close look at particular websites in SPN data
- [Liveliness]: examining whether archived content is still live

Some of the notebooks use Python extensions so you'll need to install those.
pipenv is a handy tool for managing a project's Python dependencies. These steps
should get you up and running:

    pip install pipenv
    git clone https://github.com/edsu/spn
    cd Data
    pipenv install
    pipenv shell
    jupyter notebook

Note: if you are using a notebook that requires Spark you'll need to set these
in your environment before starting Jupyter:

    export PYSPARK_DRIVER_PYTHON=jupyter
    export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
    jupyter notebook Spark.ipynb

**Utilities:**

- check.py: a utility to ensure that the downloaded files are complete

[Archival Novelty]: https://github.com/edsu/spn/blob/master/notebooks/Archival%20Novelty.ipynb
[Domains]: https://github.com/edsu/spn/blob/master/notebooks/Domains.ipynb
[Known Sites]: https://github.com/edsu/spn/blob/master/notebooks/Known%20Sites.ipynb
[Sample]: https://github.com/edsu/spn/blob/master/notebooks/Sample.ipynb
[Sizes]: https://github.com/edsu/spn/blob/master/notebooks/Sizes.ipynb
[Spark]: https://github.com/edsu/spn/blob/master/notebooks/Spark.ipynb
[Tracery]: https://github.com/edsu/spn/blob/master/notebooks/Tracery.ipynb
[URLs]: https://github.com/edsu/spn/blob/master/notebooks/URLs.ipynb
[UserAgents]: https://github.com/edsu/spn/blob/master/notebooks/UserAgents.ipynb
[WSDL Diversity Index]: https://github.com/edsu/spn/blob/master/notebooks/WSDL%20Diversity%20Index.ipynb
[Liveliness]: https://github.com/edsu/spn/blob/master/notebooks/Liveliness.ipynb

[Save Page Now]: https://wayback.archive.org

