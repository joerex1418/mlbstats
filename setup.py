import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='SimpleStatsMLB',
    version='1.0.0',
    author='Joe Rechenmacher',
    author_email='joe.rechenmacher@gmail.com',
    long_description=long_description,
    # url='https://github.com/joerex1418/cta',
    # project_urls = {
    #     "Issues": "https://github.com/joerex1418/simplestats/issues",
    #     "Projects": "https://github.com/joerex1418/simplestats/projects"
    # },
    license='GPU',
    packages=setuptools.find_packages(where='/simplestats-mlb/',include=["mlb"]),
    install_requires=['requests','pandas','beautifulsoup4','async','aiohttp'],
)
