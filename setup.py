from setuptools import setup, find_packages

setup(name='python-spark-advanced',
      version='0.1',
      url='https://proba-v-mep.esa.int',
      author="Jan Van den bosch",
      author_email='jan.van.den.bosch@devoteam.com',
      packages=find_packages(),
      setup_requires=['numpy'],
      install_requires=['numpy', 'rasterio==0.36', 'catalogclient'])

